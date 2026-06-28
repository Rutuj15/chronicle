"""The distributed Worker: polls the Engine for activities and runs them.

A Worker owns only the activity code. It long-polls the Engine
(``PollActivityTask``) for the next unit of work, runs it under its
:class:`~chronicle.runtime.ActivitySpec` policy (retry / timeout / idempotency)
via the *same* :func:`chronicle.runtime._run_activity` loop the in-process
executor uses, and reports the terminal outcome back
(``ReportActivityResult``) -- exactly one result OR one failure, never a stream
of attempts, because retry happens here.

This is where the execution policy lives, by design: next to the activity fn,
reusing :func:`_run_activity`. The Engine always sends an
``idempotency_key`` ``"{workflow_id}:{seq}"``; the Worker injects it as
``idempotency_key=`` only when its local spec is idempotent, so the Engine never
needs to know which activities are idempotent -- the separation Temporal makes.

The loop runs forever and is stopped by cancelling its task (the in-flight poll
RPC is cancelled, ending the loop). A worker processes one task at a time
(serial); within-workflow concurrency across activities is served by running
multiple workers. A task whose activity this worker lacks is *nacked*
(:rpc:`ReleaseActivityTask`) rather than run or fatal -- the lease is released so
another worker (one that has the activity, e.g. mid rolling-deploy) takes it,
and this worker keeps polling. A lost worker (process death) is the other half
of at-least-once delivery: its lease simply expires and the Engine redelivers.

Transient Engine unavailability (a restart, a deploy, a network blip) surfaces as
gRPC UNAVAILABLE on every call; each Engine RPC is retried with backoff
(:func:`_rpc`) so the Worker reconnects once the Engine is back rather than
dying on the first failure. That is what makes the Worker half of crash
recovery: an Engine that bounces does not bounce every Worker.
"""

import asyncio
import json
from collections.abc import Awaitable, Callable, Mapping

import grpc.aio

from .proto import chronicle_pb2 as pb
from .proto import chronicle_pb2_grpc as pb_grpc
from .runtime import (
    ActivityRegistry,
    ActivitySpec,
    AsyncSleeper,
    _normalize_registry,
    _run_activity,
)

# gRPC codes that mean "transient -- retry": UNAVAILABLE is what the channel
# surfaces while the Engine is down or restarting (and on a network blip). Retrying
# it with backoff is what lets a Worker ride through an Engine restart instead of
# dying on the first failed RPC. Every other code (NOT_FOUND, INVALID_ARGUMENT, ...)
# is a real error and propagates -- those are bugs, not blips.
_TRANSIENT: frozenset[grpc.StatusCode] = frozenset({grpc.StatusCode.UNAVAILABLE})


async def _rpc[T](
    call: Callable[[], Awaitable[T]],
    *,
    sleep: AsyncSleeper,
    backoff: float,
) -> T:
    """Run a gRPC ``call``, retrying transient UNAVAILABLE errors with backoff.

    The Worker outlives an Engine restart: while the Engine is down every RPC
    surfaces UNAVAILABLE, so we back off and try again rather than letting the
    poll loop die on the first failure. Retrying a *report* is correct in every
    case -- a live Engine still has the parked Future (the report resolves it),
    while a restarted Engine has re-issued the activity under a new ``task_id``
    (so the stale report is a harmless no-op). Runs until the call succeeds or
    the Worker task is cancelled.
    """
    while True:
        try:
            return await call()
        except grpc.aio.AioRpcError as exc:
            if exc.code() not in _TRANSIENT:
                raise
            await sleep(backoff)


async def run_worker(
    stub: pb_grpc.ChronicleStub,
    registry: ActivityRegistry,
    *,
    sleep: AsyncSleeper = asyncio.sleep,
    nack_retry_after: float = 0.0,
    rpc_backoff: float = 1.0,
) -> None:
    """Poll the Engine forever, running each activity under its spec's policy.

    ``sleep`` backs the retry backoff inside :func:`_run_activity` (a plain wait,
    not a recorded command) -- inject a fake to assert backoff schedules without
    real wall-clock waiting. ``nack_retry_after`` is the delay before a nacked
    task (one whose activity this worker lacks) becomes visible again, biasing
    redelivery toward another worker and damping a same-worker busy-loop when no
    worker has the activity yet.

    ``rpc_backoff`` is how long to wait between retries when an Engine RPC fails
    transiently (UNAVAILABLE) -- the cadence at which a Worker re-checks a
    restarting Engine. Without it a Worker would die on the first failed poll or
    report; with it the Worker backs off and reconnects once the Engine is back,
    so a deploy or crash that bounces the Engine does not bounce every Worker.

    Runs until the task is cancelled (teardown).
    """
    specs = _normalize_registry(registry)
    while True:
        # The Engine long-polls server-side, so an idle worker blocks here in the
        # RPC rather than busy-looping. An empty `task` (poll timeout) -> re-poll.
        # _rpc retries UNAVAILABLE so a Worker survives an Engine restart.
        response = await _rpc(
            lambda: stub.PollActivityTask(pb.PollActivityTaskRequest(task_queue="")),
            sleep=sleep,
            backoff=rpc_backoff,
        )
        if not response.HasField("task"):
            continue
        await _run_one(
            stub,
            response.task,
            specs,
            sleep=sleep,
            nack_retry_after=nack_retry_after,
            rpc_backoff=rpc_backoff,
        )


async def _run_one(
    stub: pb_grpc.ChronicleStub,
    task: pb.ActivityTask,
    specs: Mapping[str, ActivitySpec],
    *,
    sleep: AsyncSleeper,
    nack_retry_after: float,
    rpc_backoff: float,
) -> None:
    """Run one activity task under its policy and report the terminal outcome.

    The marker contract is honored exactly: only :func:`_run_activity`'s
    exhausted failure becomes a ``failure`` report; everything else propagates.
    A task whose activity this worker lacks is *nacked* (released for another
    worker) rather than reported as a lying failure or allowed to kill the loop
    -- a missing activity is a deployment state, not an activity outcome.

    Each Engine RPC (release, failure report, result report) goes through
    :func:`_rpc`, so a transient UNAVAILABLE during an Engine restart is retried
    rather than dropped: the Worker holds the result and redelivers it once the
    Engine is back, instead of losing the work or dying.
    """
    if task.activity_name not in specs:
        # This worker cannot action the task (a rolling deploy: another worker
        # may have the activity). Release the lease so another worker gets a
        # shot, then keep polling -- do NOT report a Failed (that would be a
        # lie) and do NOT die (one un-actionable task must not brick a worker).
        release = pb.ReleaseActivityTaskRequest(
            task_id=task.task_id, retry_after_seconds=nack_retry_after
        )
        await _rpc(
            lambda: stub.ReleaseActivityTask(release), sleep=sleep, backoff=rpc_backoff
        )
        return
    spec = specs[task.activity_name]
    # The Engine always sends the key; inject it only if this activity is
    # registered idempotent, so a non-idempotent activity never sees the kwarg.
    key = task.idempotency_key if spec.idempotent else None
    args = tuple(json.loads(task.args_json))
    try:
        result = await _run_activity(spec, args, key=key, sleep=sleep)
    except Exception as exc:
        # Retry/timeout exhausted: one terminal failure. BaseException
        # (KeyboardInterrupt/SystemExit, cancellation) is not caught here, so it
        # propagates untouched -- the Engine's parked future waits for
        # redelivery if the worker dies mid-report, handled by leasing. Build the
        # report while ``exc`` is in scope (an ``except ... as`` target is unbound
        # once the clause exits), then hand the ready request to ``_rpc``.
        report = pb.ReportActivityResultRequest(
            task_id=task.task_id,
            failure=pb.ActivityFailure(
                error_type=type(exc).__name__, error_message=str(exc)
            ),
        )
        await _rpc(
            lambda: stub.ReportActivityResult(report), sleep=sleep, backoff=rpc_backoff
        )
        return
    report = pb.ReportActivityResultRequest(
        task_id=task.task_id, result_json=json.dumps(result)
    )
    await _rpc(
        lambda: stub.ReportActivityResult(report), sleep=sleep, backoff=rpc_backoff
    )


__all__ = ["run_worker"]
