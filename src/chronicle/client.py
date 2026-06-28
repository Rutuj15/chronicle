"""The Client SDK: start workflows and observe their results over gRPC.

The Client is the user-facing surface of the distributed engine -- a thin typed
wrapper over ``StartWorkflow`` / ``GetWorkflowResult``. It hides the gRPC stubs
and proto types entirely: a caller starts a workflow by id + name + args and
gets back a :class:`WorkflowResult` carrying a status enum, the deserialized
result, and structured error fields. No proto type leaks into the SDK API.

``get_result`` long-polls: the Engine reports ``RUNNING`` when its server-side
window elapses without a terminal state, and the Client re-calls until the
workflow finishes (or its own budget runs out, in which case it returns the last
``RUNNING``). So a result may be ``COMPLETED``, ``FAILED``, or ``RUNNING`` (the
last only if the caller's ``timeout`` elapsed mid-flight).
"""

import asyncio
import json
from dataclasses import dataclass
from enum import Enum

import grpc.aio

from .events import JsonValue
from .proto import chronicle_pb2 as pb
from .proto import chronicle_pb2_grpc as pb_grpc


class WorkflowStatus(Enum):
    """A workflow's state, mirroring the wire enum without leaking the proto type."""

    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


@dataclass(frozen=True)
class WorkflowResult:
    """The terminal (or, in 3c, in-progress) outcome of a workflow.

    ``result`` is the deserialized return value on ``COMPLETED`` (``None`` if the
    workflow returned nothing, since ``None`` is a valid JSON value); the error
    fields are populated on ``FAILED``.
    """

    status: WorkflowStatus
    result: JsonValue | None = None
    error_type: str | None = None
    error_message: str | None = None


class Client:
    """A typed Chronicle client over a gRPC channel.

    The channel is *injected and owned by the caller* (open and close it
    yourself); the Client just holds the stub built from it.
    """

    def __init__(self, channel: grpc.aio.Channel) -> None:
        # ChronicleStub is generated (chronicle_pb2_grpc) with no type hints, so
        # constructing it is an untyped call -- the one justified ignore for the
        # generated stub (the message constructors above are typed via their .pyi).
        self._stub = pb_grpc.ChronicleStub(channel)  # type: ignore[no-untyped-call]

    async def start_workflow(
        self,
        workflow_id: str,
        workflow_name: str,
        *args: JsonValue,
    ) -> None:
        """Start ``workflow_name`` as ``workflow_id`` with ``args``; return at once.

        ``args`` is JSON-encoded as a tuple and round-trips back to one in the
        Engine, preserving tuple fidelity. The workflow runs in the Engine after
        this returns; call :meth:`get_result` to observe the outcome.
        """
        await self._stub.StartWorkflow(
            pb.StartWorkflowRequest(
                workflow_id=workflow_id,
                workflow_name=workflow_name,
                args_json=json.dumps(args),
            )
        )

    async def get_result(
        self,
        workflow_id: str,
        *,
        timeout: float | None = None,
    ) -> WorkflowResult:
        """Long-poll ``workflow_id`` until it finishes, then return its outcome.

        The Engine long-polls server-side up to its window, returning ``RUNNING``
        if the workflow hasn't finished; this re-calls until a terminal state or
        the overall ``timeout`` budget elapses (then the last ``RUNNING`` is
        returned, so the caller may observe an in-flight workflow without raising).
        ``timeout`` caps the TOTAL wait across re-polls (``None`` = wait
        indefinitely); each gRPC call's deadline is the budget remaining.
        """
        loop = asyncio.get_running_loop()
        deadline = None if timeout is None else loop.time() + timeout
        while True:
            remaining = None if deadline is None else deadline - loop.time()
            if remaining is not None and remaining <= 0:
                # Budget elapsed mid-flight: surface RUNNING rather than raise --
                # the caller asked to observe, not to enforce a hard deadline.
                return WorkflowResult(status=WorkflowStatus.RUNNING)
            try:
                response = await self._stub.GetWorkflowResult(
                    pb.GetWorkflowResultRequest(workflow_id=workflow_id),
                    timeout=remaining,
                )
            except grpc.aio.AioRpcError as exc:
                # The observe call hit its gRPC deadline: the workflow is still
                # running. Re-poll if budget remains, else (next iteration) return
                # RUNNING. This absorbs the boundary race where the remaining
                # budget slips under the server's long-poll window, so a tight
                # client timeout surfaces RUNNING instead of an error.
                if exc.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
                    continue
                raise
            result = _from_response(response)
            if result.status is not WorkflowStatus.RUNNING:
                return result
            # Server's long-poll window elapsed without a terminal state; re-poll.


def _from_response(response: pb.GetWorkflowResultResponse) -> WorkflowResult:
    """Translate the wire response into a :class:`WorkflowResult`."""
    if response.status == pb.GetWorkflowResultResponse.COMPLETED:
        return WorkflowResult(
            status=WorkflowStatus.COMPLETED,
            result=json.loads(response.result_json),
        )
    if response.status == pb.GetWorkflowResultResponse.FAILED:
        return WorkflowResult(
            status=WorkflowStatus.FAILED,
            error_type=response.error_type or None,
            error_message=response.error_message or None,
        )
    return WorkflowResult(status=WorkflowStatus.RUNNING)


__all__ = ["Client", "WorkflowResult", "WorkflowStatus"]
