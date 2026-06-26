from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class StartWorkflowRequest(_message.Message):
    __slots__ = ("workflow_id", "workflow_name", "args_json")
    WORKFLOW_ID_FIELD_NUMBER: _ClassVar[int]
    WORKFLOW_NAME_FIELD_NUMBER: _ClassVar[int]
    ARGS_JSON_FIELD_NUMBER: _ClassVar[int]
    workflow_id: str
    workflow_name: str
    args_json: str
    def __init__(self, workflow_id: _Optional[str] = ..., workflow_name: _Optional[str] = ..., args_json: _Optional[str] = ...) -> None: ...

class StartWorkflowResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class GetWorkflowResultRequest(_message.Message):
    __slots__ = ("workflow_id",)
    WORKFLOW_ID_FIELD_NUMBER: _ClassVar[int]
    workflow_id: str
    def __init__(self, workflow_id: _Optional[str] = ...) -> None: ...

class GetWorkflowResultResponse(_message.Message):
    __slots__ = ("status", "result_json", "error_type", "error_message")
    class Status(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        RUNNING: _ClassVar[GetWorkflowResultResponse.Status]
        COMPLETED: _ClassVar[GetWorkflowResultResponse.Status]
        FAILED: _ClassVar[GetWorkflowResultResponse.Status]
    RUNNING: GetWorkflowResultResponse.Status
    COMPLETED: GetWorkflowResultResponse.Status
    FAILED: GetWorkflowResultResponse.Status
    STATUS_FIELD_NUMBER: _ClassVar[int]
    RESULT_JSON_FIELD_NUMBER: _ClassVar[int]
    ERROR_TYPE_FIELD_NUMBER: _ClassVar[int]
    ERROR_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    status: GetWorkflowResultResponse.Status
    result_json: str
    error_type: str
    error_message: str
    def __init__(self, status: _Optional[_Union[GetWorkflowResultResponse.Status, str]] = ..., result_json: _Optional[str] = ..., error_type: _Optional[str] = ..., error_message: _Optional[str] = ...) -> None: ...

class PollActivityTaskRequest(_message.Message):
    __slots__ = ("task_queue",)
    TASK_QUEUE_FIELD_NUMBER: _ClassVar[int]
    task_queue: str
    def __init__(self, task_queue: _Optional[str] = ...) -> None: ...

class ActivityTask(_message.Message):
    __slots__ = ("task_id", "workflow_id", "activity_name", "args_json", "idempotency_key")
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    WORKFLOW_ID_FIELD_NUMBER: _ClassVar[int]
    ACTIVITY_NAME_FIELD_NUMBER: _ClassVar[int]
    ARGS_JSON_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    task_id: str
    workflow_id: str
    activity_name: str
    args_json: str
    idempotency_key: str
    def __init__(self, task_id: _Optional[str] = ..., workflow_id: _Optional[str] = ..., activity_name: _Optional[str] = ..., args_json: _Optional[str] = ..., idempotency_key: _Optional[str] = ...) -> None: ...

class PollActivityTaskResponse(_message.Message):
    __slots__ = ("task",)
    TASK_FIELD_NUMBER: _ClassVar[int]
    task: ActivityTask
    def __init__(self, task: _Optional[_Union[ActivityTask, _Mapping]] = ...) -> None: ...

class ReportActivityResultRequest(_message.Message):
    __slots__ = ("task_id", "result_json", "failure")
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    RESULT_JSON_FIELD_NUMBER: _ClassVar[int]
    FAILURE_FIELD_NUMBER: _ClassVar[int]
    task_id: str
    result_json: str
    failure: ActivityFailure
    def __init__(self, task_id: _Optional[str] = ..., result_json: _Optional[str] = ..., failure: _Optional[_Union[ActivityFailure, _Mapping]] = ...) -> None: ...

class ActivityFailure(_message.Message):
    __slots__ = ("error_type", "error_message")
    ERROR_TYPE_FIELD_NUMBER: _ClassVar[int]
    ERROR_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    error_type: str
    error_message: str
    def __init__(self, error_type: _Optional[str] = ..., error_message: _Optional[str] = ...) -> None: ...

class ReportActivityResultResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...
