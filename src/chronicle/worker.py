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
"""

import asyncio
import json
from collections.abc import Mapping

from .proto import chronicle_pb2 as pb
from .proto import chronicle_pb2_grpc as pb_grpc
from .runtime import (
    ActivityRegistry,
    ActivitySpec,
    AsyncSleeper,
    _normalize_registry,
    _run_activity,
)


async def run_worker(
    stub: pb_grpc.ChronicleStub,
    registry: ActivityRegistry,
    *,
    sleep: AsyncSleeper = asyncio.sleep,
    nack_retry_after: float = 0.0,
) -> None:
    """Poll the Engine forever, running each activity under its spec's policy.

    ``sleep`` backs the retry backoff inside :func:`_run_activity` (a plain wait,
    not a recorded command) -- inject a fake to assert backoff schedules without
    real wall-clock waiting. ``nack_retry_after`` is the delay before a nacked
    task (one whose activity this worker lacks) becomes visible again, biasing
    redelivery toward another worker and damping a same-worker busy-loop when no
    worker has the activity yet. Runs until the task is cancelled (teardown).
    """
    specs = _normalize_registry(registry)
    while True:
        # The Engine long-polls server-side, so an idle worker blocks here in the
        # RPC rather than busy-looping. An empty `task` (poll timeout) -> re-poll.
        response = await stub.PollActivityTask(pb.PollActivityTaskRequest(task_queue=""))
        if not response.HasField("task"):
            continue
        await _run_one(
            stub, response.task, specs, sleep=sleep, nack_retry_after=nack_retry_after
        )


async def _run_one(
    stub: pb_grpc.ChronicleStub,
    task: pb.ActivityTask,
    specs: Mapping[str, ActivitySpec],
    *,
    sleep: AsyncSleeper,
    nack_retry_after: float,
) -> None:
    """Run one activity task under its policy and report the terminal outcome.

    The marker contract is honored exactly: only :func:`_run_activity`'s
    exhausted failure becomes a ``failure`` report; everything else propagates.
    A task whose activity this worker lacks is *nacked* (released for another
    worker) rather than reported as a lying failure or allowed to kill the loop
    -- a missing activity is a deployment state, not an activity outcome.
    """
    if task.activity_name not in specs:
        # This worker cannot action the task (a rolling deploy: another worker
        # may have the activity). Release the lease so another worker gets a
        # shot, then keep polling -- do NOT report a Failed (that would be a
        # lie) and do NOT die (one un-actionable task must not brick a worker).
        await stub.ReleaseActivityTask(
            pb.ReleaseActivityTaskRequest(
                task_id=task.task_id, retry_after_seconds=nack_retry_after
            )
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
        # redelivery if the worker dies mid-report, handled by leasing.
        await stub.ReportActivityResult(
            pb.ReportActivityResultRequest(
                task_id=task.task_id,
                failure=pb.ActivityFailure(
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                ),
            )
        )
        return
    await stub.ReportActivityResult(
        pb.ReportActivityResultRequest(
            task_id=task.task_id,
            result_json=json.dumps(result),
        )
    )


__all__ = ["run_worker"]
