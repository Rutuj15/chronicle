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

In 3b the per-workflow event log is in memory: the live coroutine IS the
workflow's state. Engine-log durability + live-coroutine reconstruction on an
engine crash. The task queue is durable on purpose -- the foundation
(worker crash recovery) build on.
"""

import asyncio
import contextlib
import json
from collections.abc import Callable, Coroutine, Mapping
from typing import Any
from uuid import uuid4

import grpc.aio

from .events import JsonValue
from .history import InMemoryEventLog
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
        *,
        poll_timeout: float = 5.0,
    ) -> None:
        self._workflows = workflows
        self._queue = queue
        # How long PollActivityTask blocks for work before returning empty (the
        # worker then re-polls). Long enough to be efficient, short enough that a
        # worker shutting down re-polls promptly.
        self._poll_timeout = poll_timeout
        # task_id -> the Future the parked run() is awaiting. Shared by every
        # RemoteActivityExecutor call; resolved by ReportActivityResult.
        self._pending: dict[str, asyncio.Future[JsonValue]] = {}
        self._executor = RemoteActivityExecutor(queue, self._pending)
        # workflow_id -> the live run() task. The live coroutine is the
        # workflow's state (in-memory in 3b).
        self._runs: dict[str, asyncio.Task[JsonValue]] = {}

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
        ``workflow_name`` aborts.
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
        self._runs[workflow_id] = asyncio.create_task(
            run(
                workflow,
                args,
                InMemoryEventLog(),
                executor=self._executor,
                workflow_id=workflow_id,
            ),
            name=f"chronicle-workflow:{workflow_id}",
        )
        return pb.StartWorkflowResponse()

    async def GetWorkflowResult(
        self,
        request: pb.GetWorkflowResultRequest,
        context: grpc.aio.ServicerContext,
    ) -> pb.GetWorkflowResultResponse:
        """Block until ``workflow_id`` finishes, then report its terminal outcome.

        Awaits the live ``run`` task: a return value becomes ``COMPLETED``; a
        recorded activity failure (``ActivityFailedError``) or a determinism
        violation (``NonDeterminismError``) becomes ``FAILED``. In 3b this always
        returns a terminal outcome -- the ``RUNNING`` case (long-poll timeout)
        lands with leasing in 3c.
        """
        if request.workflow_id not in self._runs:
            await context.abort(
                grpc.aio.StatusCode.NOT_FOUND,
                f"no workflow {request.workflow_id!r}",
            )
        task = self._runs[request.workflow_id]
        try:
            result = await task
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
        """Resolve the parked Future for ``task_id`` with the Worker's outcome.

        The Worker has already applied its retry/timeout policy, so this is
        exactly one result OR one failure. On failure the marker
        (``_ActivityExecutionError``) is reconstituted and set as the Future's
        exception, so ``_execute`` records one ``Failed`` exactly as an in-process
        failure would. An unknown ``task_id`` (already resolved, or a stale
        duplicate) is a no-op -- the de-dup home for redelivery is leasing, 3c.
        """
        fut = self._pending.get(request.task_id)
        if fut is None or fut.done():
            return pb.ReportActivityResultResponse()
        outcome = request.WhichOneof("outcome")
        if outcome == "failure":
            fut.set_exception(
                _ActivityExecutionError(
                    request.failure.error_type, request.failure.error_message
                )
            )
        elif outcome == "result_json":
            fut.set_result(json.loads(request.result_json))
        # An unset outcome (malformed report) resolves nothing; the Worker will
        # time out and the task redelivers once leasing exists.
        return pb.ReportActivityResultResponse()

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
