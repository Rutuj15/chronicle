"""The distributed Engine: workflow replay + coordination over gRPC.

The Engine is the durable "brain": it owns the workflow registry, drives each
workflow's replay/determinism loop (:func:`chronicle.runtime.run`, unchanged),
holds the live workflow coroutines, and brokers activity results back to them.
Workers own only the activity code: they poll the Engine for a unit of work,
run it, and report the outcome -- and the Engine resolves the parked workflow
coroutine that issued it.

This module is the bridge between :func:`run` (unchanged) and the wire:

* :class:`RemoteActivityExecutor` -- the :class:`~chronicle.runtime.ActivityExecutor`
  that ``run`` drives. For each activity it mints a ``task_id``, parks on an
  :class:`asyncio.Future` keyed by that id, and enqueues the work.
  :meth:`Engine.ReportActivityResult` resolves that Future, resuming ``run``.
* :class:`Engine` (the gRPC servicer) -- ``StartWorkflow`` spawns a ``run`` task;
  ``GetWorkflowResult`` awaits it; ``PollActivityTask`` hands out queued work;
  ``ReportActivityResult`` resolves the parked Future.

The marker that distinguishes "the activity ran and failed" from a setup error
(:class:`chronicle.runtime._ActivityExecutionError`) is *reconstituted* on the
Engine side from the worker's failure report, so :func:`_execute` records exactly
one ``Failed`` exactly as in-process -- the determinism + failure model is
identical across the process boundary.

The per-workflow event log is DURABLE (a :class:`~chronicle.history.SqliteEventLog`
over the engine's own aiosqlite connection): the live coroutine is a *cache* of
replay state, and the recorded history is the source of truth. On a restart,
:meth:`Engine.start` -> :meth:`Engine.recover` replays each workflow's history
into a fresh live ``run`` coroutine, resuming exactly where it left off --
Temporal's replay-on-restart insight, engine-owned. The task queue is durable
AND leased (3c): a taken task is hidden, not deleted, so a lost worker's lease
expires and the task redelivers to another; and an engine that crashes
mid-activity simply re-issues the activity on recovery (the stale pre-crash task
self-cleans). Both are at-least-once, which is what the engine-minted
idempotency keys make safe.
"""

import asyncio
import contextlib
import json
from collections.abc import Callable, Coroutine, Mapping
from typing import Any
from uuid import uuid4

import aiosqlite
import grpc.aio

from .events import JsonValue
from .history import SqliteEventLog
from .proto import chronicle_pb2 as pb
from .proto import chronicle_pb2_grpc as pb_grpc
from .retry import idempotency_key
from .runtime import (
    ActivityFailedError,
    NonDeterminismError,
    _ActivityExecutionError,
    run,
)
from .task_queue import TaskQueue

# A workflow is a coroutine-producing callable driven by run(); it returns a
# JSON value. The Engine selects one by name from this registry on StartWorkflow.
WorkflowFn = Callable[..., Coroutine[Any, Any, JsonValue]]


class RemoteActivityExecutor:
    """An :class:`~chronicle.runtime.ActivityExecutor` that dispatches to a Worker.

    This is the distribution half of the executor seam: ``run`` drives it exactly
    as it drives :class:`~chronicle.runtime.LocalActivityExecutor`, but instead of
    running the activity in-process it parks on an :class:`asyncio.Future` and
    lets a Worker do the work over the wire. Each call:

    1. mints a unique ``task_id`` (keys the parked Future, so the right workflow
       resumes when the report arrives);
    2. enqueues an :class:`ActivityTask` carrying the engine-minted idempotency
       key ``"{workflow_id}:{seq}"`` -- always sent, whether or not the activity
       is idempotent (the Worker decides that from its own registry);
    3. awaits the Future. :meth:`Engine.ReportActivityResult` resolves it with
       the result or raises ``_ActivityExecutionError`` (which ``_execute`` turns
       into one ``Failed``) -- so replay and failure handling are identical to
       in-process.

    The ``pending`` map is shared across all workflows (task_id is globally
    unique), so a single executor instance serves every workflow the Engine runs.
    """

    def __init__(
        self,
        queue: TaskQueue,
        pending: dict[str, asyncio.Future[JsonValue]],
    ) -> None:
        self._queue = queue
        self._pending = pending

    async def execute(
        self,
        name: str,
        args: tuple[JsonValue, ...],
        *,
        workflow_id: str | None,
        seq: int,
    ) -> JsonValue:
        # The distributed engine always has a workflow_id (the Client supplies
        # one), so the idempotency key is always mintable. Guard None here both
        # to fail loudly and to narrow the type for idempotency_key().
        if workflow_id is None:
            raise RuntimeError("the distributed engine always supplies a workflow_id")
        task_id = uuid4().hex
        fut: asyncio.Future[JsonValue] = asyncio.get_running_loop().create_future()
        self._pending[task_id] = fut
        task = pb.ActivityTask(
            task_id=task_id,
            workflow_id=workflow_id,
            activity_name=name,
            args_json=json.dumps(args),
            # Engine-minted and always sent; the Worker injects it only if its
            # local spec is idempotent, so the Engine never needs to know which.
            idempotency_key=idempotency_key(workflow_id, seq),
        )
        try:
            await self._queue.enqueue(task)
            return await fut
        finally:
            # Drop the entry whether the activity succeeded, failed, or the
            # workflow was cancelled -- never leak a resolved/cancelled Future.
            self._pending.pop(task_id, None)


class Engine(pb_grpc.ChronicleServicer):
    """The gRPC Chronicle service: runs workflows and coordinates Workers.

    Construct with a workflow registry (name -> coroutine fn) and a
    :class:`TaskQueue`; attach to a ``grpc.aio`` server via
    ``add_ChronicleServicer_to_server``. The Engine builds one shared
    :class:`RemoteActivityExecutor` over the queue and the pending-Future map,
    and spawns a fresh ``run`` task per ``StartWorkflow``.

    A started workflow runs as a live coroutine held in ``self._runs``;
    ``GetWorkflowResult`` awaits it. The pending map (``task_id -> Future``) is
    the rendezvous between activities parked in ``run`` and results reported by
    Workers.
    """

    def __init__(
        self,
        workflows: Mapping[str, WorkflowFn],
        queue: TaskQueue,
        event_conn: aiosqlite.Connection,
        *,
        poll_timeout: float = 5.0,
        result_timeout: float = 5.0,
    ) -> None:
        self._workflows = workflows
        self._queue = queue
        # The engine's durable state -- each workflow's event log AND its
        # identity (name + args) -- lives on this connection, so both survive an
        # engine crash. The lock serializes multi-call sequences on it (the 3b
        # aiosqlite lesson: a cursor open across an await + a concurrent commit
        # fails with "SQL statements in progress").
        self._conn = event_conn
        self._lock = asyncio.Lock()
        # How long PollActivityTask blocks for work before returning empty (the
        # worker then re-polls). Long enough to be efficient, short enough that a
        # worker shutting down re-polls promptly.
        self._poll_timeout = poll_timeout
        # How long GetWorkflowResult waits for the workflow to finish before
        # returning RUNNING (the client then re-polls). A server-side long-poll
        # window, capped by the call's gRPC deadline.
        self._result_timeout = result_timeout
        # task_id -> the Future the parked run() is awaiting. Shared by every
        # RemoteActivityExecutor call; resolved by ReportActivityResult.
        self._pending: dict[str, asyncio.Future[JsonValue]] = {}
        self._executor = RemoteActivityExecutor(queue, self._pending)
        # workflow_id -> the live run() task. The live coroutine is a CACHE of
        # replay state; the durable event log is the source of truth, replayed
        # back into a fresh coroutine by recover() on a restart.
        self._runs: dict[str, asyncio.Task[JsonValue]] = {}

    async def start(self) -> None:
        """Ensure the workflow-metadata table, then recover every known workflow.

        Call once at startup, before serving. recover() replays each persisted
        workflow's history into a fresh live coroutine, so an engine that crashed
        mid-workflow resumes exactly where it left off. A no-op for a brand-new
        engine (nothing persisted yet).
        """
        async with self._lock:
            await self._conn.execute(
                "CREATE TABLE IF NOT EXISTS workflows ("
                " workflow_id TEXT PRIMARY KEY,"
                " workflow_name TEXT NOT NULL,"
                " args_json TEXT NOT NULL"
                ")"
            )
            await self._conn.commit()
        await self.recover()

    async def recover(self) -> None:
        """Reconstruct every known workflow by replaying its durable history.

        For each workflow the engine has a metadata row for, drive ``run`` over
        its durable log: the recorded prefix replays (activities fed back from
        history, never re-executed) and any in-flight step resumes into new
        ground -- the same one loop, just over a non-empty log. A terminal
        workflow replays to its end and finishes at once (pure replay: no side
        effects, no re-execution). A workflow whose name is no longer registered,
        or whose args fail to decode, cannot be reconstructed and is skipped
        rather than aborting recovery.
        """
        async with self._lock:
            cur = await self._conn.execute(
                "SELECT workflow_id, workflow_name, args_json FROM workflows"
            )
            rows = await cur.fetchall()
            await cur.close()
        for workflow_id, workflow_name, args_json in rows:
            workflow = self._workflows.get(workflow_name)
            if workflow is None:
                continue  # definition removed since the workflow was started: skip
            try:
                args = tuple(json.loads(args_json))
            except (json.JSONDecodeError, TypeError):
                continue  # corrupt metadata: skip rather than abort recovery
            await self._spawn(workflow_id, workflow, args)

    async def _spawn(
        self,
        workflow_id: str,
        workflow: WorkflowFn,
        args: tuple[JsonValue, ...],
    ) -> None:
        """Drive ``run`` for ``workflow_id`` over its durable log.

        Creates the per-workflow :class:`~chronicle.history.SqliteEventLog` (a
        view of the shared event connection) and spawns ``run`` as the live
        coroutine. ``run`` replays whatever history is already on disk (nothing
        on a fresh start; a recorded prefix on recovery) and continues into new
        ground.
        """
        log = SqliteEventLog(self._conn, workflow_id, self._lock)
        await log.start()
        self._runs[workflow_id] = asyncio.create_task(
            run(
                workflow,
                args,
                log,
                executor=self._executor,
                workflow_id=workflow_id,
            ),
            name=f"chronicle-workflow:{workflow_id}",
        )

    # --- Client -> Engine ----------------------------------------------------

    async def StartWorkflow(
        self,
        request: pb.StartWorkflowRequest,
        context: grpc.aio.ServicerContext,
    ) -> pb.StartWorkflowResponse:
        """Register and start the workflow as a live coroutine; return at once.

        The Client later calls ``GetWorkflowResult`` to observe the outcome, so
        this returns immediately -- the workflow keeps running in the Engine
        after the RPC completes. A duplicate ``workflow_id`` or an unknown
        ``workflow_name`` aborts. The workflow's identity (name + args) is
        persisted durably BEFORE its run starts, so a crash at any point after
        still lets ``recover()`` know the workflow existed and replay whatever
        history reached disk.
        """
        workflow_id = request.workflow_id
        if not workflow_id:
            await context.abort(
                grpc.aio.StatusCode.INVALID_ARGUMENT, "workflow_id is required"
            )
        if workflow_id in self._runs:
            await context.abort(
                grpc.aio.StatusCode.ALREADY_EXISTS,
                f"workflow {workflow_id!r} is already running",
            )
        if request.workflow_name not in self._workflows:
            await context.abort(
                grpc.aio.StatusCode.NOT_FOUND,
                f"no workflow registered as {request.workflow_name!r}",
            )
        workflow = self._workflows[request.workflow_name]
        try:
            args = tuple(json.loads(request.args_json))
        except (json.JSONDecodeError, TypeError) as exc:
            await context.abort(
                grpc.aio.StatusCode.INVALID_ARGUMENT,
                f"args_json is not valid JSON: {exc}",
            )
        # Persist identity durably before spawning: this row is what recover()
        # reads to know the workflow existed. args_json is stored verbatim (it is
        # already JSON on the wire), so it round-trips exactly on recovery.
        async with self._lock:
            await self._conn.execute(
                "INSERT INTO workflows (workflow_id, workflow_name, args_json)"
                " VALUES (?, ?, ?)",
                (workflow_id, request.workflow_name, request.args_json),
            )
            await self._conn.commit()
        await self._spawn(workflow_id, workflow, args)
        return pb.StartWorkflowResponse()

    async def GetWorkflowResult(
        self,
        request: pb.GetWorkflowResultRequest,
        context: grpc.aio.ServicerContext,
    ) -> pb.GetWorkflowResultResponse:
        """Long-poll ``workflow_id`` for up to a budget, then report its state.

        Awaits the live ``run`` task for the server-side long-poll window (capped
        by the call's gRPC deadline). If it finishes in time, a return value
        becomes ``COMPLETED`` and a recorded activity failure
        (``ActivityFailedError``) or a determinism violation
        (``NonDeterminismError``) becomes ``FAILED``. If the budget elapses first,
        returns ``RUNNING`` so the client may re-poll. ``asyncio.wait`` is used
        (not ``wait_for``) so a budget expiry never cancels the live run -- the
        workflow keeps executing across the re-poll.
        """
        if request.workflow_id not in self._runs:
            await context.abort(
                grpc.aio.StatusCode.NOT_FOUND,
                f"no workflow {request.workflow_id!r}",
            )
        task = self._runs[request.workflow_id]
        budget = self._result_budget(context)
        if not task.done() and budget > 0:
            # asyncio.wait does NOT cancel `task` on timeout -- the live run
            # keeps going; we just stop waiting and report RUNNING. The return
            # value is unused: we re-check task.done() below instead.
            await asyncio.wait({task}, timeout=budget)
        if not task.done():
            return pb.GetWorkflowResultResponse(
                status=pb.GetWorkflowResultResponse.RUNNING
            )
        try:
            result = task.result()
        except ActivityFailedError as exc:
            return pb.GetWorkflowResultResponse(
                status=pb.GetWorkflowResultResponse.FAILED,
                error_type=exc.error_type,
                error_message=exc.error_message,
            )
        except NonDeterminismError as exc:
            return pb.GetWorkflowResultResponse(
                status=pb.GetWorkflowResultResponse.FAILED,
                error_type=type(exc).__name__,
                error_message=str(exc),
            )
        return pb.GetWorkflowResultResponse(
            status=pb.GetWorkflowResultResponse.COMPLETED,
            result_json=json.dumps(result),
        )

    def _result_budget(self, context: grpc.aio.ServicerContext) -> float:
        """The long-poll window for GetWorkflowResult, capped by the gRPC deadline.

        ``context.time_remaining()`` is ``None`` when the client set no deadline;
        otherwise cap the server-side window so we never outlive the call.
        """
        remaining = context.time_remaining()
        if remaining is None:
            return self._result_timeout
        # gRPC returns the deadline in seconds (a float) or None; grpc.aio has no
        # stubs so the call is Any -- coerce to satisfy the float return type.
        return min(self._result_timeout, float(remaining))

    # --- Worker -> Engine ----------------------------------------------------

    async def PollActivityTask(
        self,
        request: pb.PollActivityTaskRequest,
        context: grpc.aio.ServicerContext,
    ) -> pb.PollActivityTaskResponse:
        """Hand the Worker the next activity, or block (long-poll) then return empty.

        ``request.task_queue`` is accepted but 3b has a single (default) queue;
        named queues land with leasing in 3c. An empty ``task`` field means "no
        work right now" and the Worker re-polls.
        """
        task = await self._queue.poll(timeout=self._poll_timeout)
        if task is None:
            return pb.PollActivityTaskResponse()
        return pb.PollActivityTaskResponse(task=task)

    async def ReportActivityResult(
        self,
        request: pb.ReportActivityResultRequest,
        context: grpc.aio.ServicerContext,
    ) -> pb.ReportActivityResultResponse:
        """Resolve the parked Future for ``task_id`` and complete its lease.

        The Worker has already applied its retry/timeout policy, so this is
        exactly one result OR one failure. On failure the marker
        (``_ActivityExecutionError``) is reconstituted and set as the Future's
        exception, so ``_execute`` records one ``Failed`` exactly as an in-process
        failure would. Either terminal outcome also completes the lease
        (:meth:`TaskQueue.complete`) -- the durable signal a worker finished, so
        the task is never redelivered. A report for an unknown / already-resolved
        ``task_id`` (a stale duplicate from a faster delivery) resolves nothing
        but still completes idempotently. An unset outcome (a malformed report)
        resolves AND completes nothing -- the lease then expires and the task
        redelivers.
        """
        fut = self._pending.get(request.task_id)
        outcome = request.WhichOneof("outcome")
        if outcome == "failure":
            if fut is not None and not fut.done():
                fut.set_exception(
                    _ActivityExecutionError(
                        request.failure.error_type, request.failure.error_message
                    )
                )
            await self._queue.complete(request.task_id)
        elif outcome == "result_json":
            if fut is not None and not fut.done():
                fut.set_result(json.loads(request.result_json))
            await self._queue.complete(request.task_id)
        # An unset outcome (malformed report): resolve nothing, complete nothing
        # -- the lease expires and the task redelivers (honest, now that leasing
        # exists, rather than wedging the parked Future forever as in 3b).
        return pb.ReportActivityResultResponse()

    async def ReleaseActivityTask(
        self,
        request: pb.ReleaseActivityTaskRequest,
        context: grpc.aio.ServicerContext,
    ) -> pb.ReleaseActivityTaskResponse:
        """Re-queue a task a Worker declined (a nack) -- make it visible again.

        A Worker that cannot action a task (e.g. it lacks the activity during a
        rolling deploy) calls this instead of reporting a result, and keeps
        polling. The Engine releases the lease so another Worker gets a shot
        promptly rather than waiting for the lease to expire. The parked Future
        stays parked, waiting for whichever delivery eventually reports. A no-op
        if the task was already completed by another delivery.
        """
        await self._queue.release(
            request.task_id, retry_after=request.retry_after_seconds
        )
        return pb.ReleaseActivityTaskResponse()

    # --- Lifecycle -----------------------------------------------------------

    async def stop(self) -> None:
        """Cancel every live workflow task (best-effort cleanup for shutdown)."""
        tasks = list(self._runs.values())
        for task in tasks:
            task.cancel()
        for task in tasks:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
        self._runs.clear()


__all__ = ["Engine", "RemoteActivityExecutor", "WorkflowFn"]
