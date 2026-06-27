"""The Client SDK: start workflows and observe their results over gRPC.

The Client is the user-facing surface of the distributed engine -- a thin typed
wrapper over ``StartWorkflow`` / ``GetWorkflowResult``. It hides the gRPC stubs
and proto types entirely: a caller starts a workflow by id + name + args and
gets back a :class:`WorkflowResult` carrying a status enum, the deserialized
result, and structured error fields. No proto type leaks into the SDK API.

In 3b ``get_result`` blocks until the workflow reaches a terminal state, so a
result is always ``COMPLETED`` or ``FAILED``; the ``RUNNING`` case (a long-poll
timeout returning early) lands with leasing in 3c.
"""

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
        """Block until ``workflow_id`` finishes, then return its outcome.

        ``timeout`` caps the wait (a gRPC deadline); the default ``None`` waits
        until the Engine reports a terminal state. The Engine's
        ``GetWorkflowResult`` blocks server-side until the workflow is done, so in
        3b the result is always ``COMPLETED`` or ``FAILED``.
        """
        response = await self._stub.GetWorkflowResult(
            pb.GetWorkflowResultRequest(workflow_id=workflow_id),
            timeout=timeout,
        )
        return _from_response(response)


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
