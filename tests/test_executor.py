"""The ActivityExecutor seam.

``run`` can take an explicit ``executor=`` instead of a registry: the loop drives
the workflow exactly as before, but each activity is run by that executor rather
than in-process. This is the seam a remote executor plugs into -- dispatching each activity
to a worker process -- so these tests pin the contract every executor
must meet: it is called per activity with the name, args, workflow_id, and seq;
its return value flows back into the workflow; and only an execution-failure
signal becomes a recorded ``Failed`` -- anything else it raises propagates
untouched, because that is a setup error, not an activity outcome.
"""

import pytest

from chronicle.context import WorkflowContext
from chronicle.events import JsonValue
from chronicle.history import InMemoryEventLog
from conftest import run_sync


async def _call_activity(ctx: WorkflowContext, name: str, *args: JsonValue) -> JsonValue:
    return await ctx.activity(name, *args)


async def _succeed() -> None:
    """A placeholder activity; only used where it is never actually run."""
    return None


class _RecordingExecutor:
    """A stand-in executor: returns canned results and records every call.

    Deliberately NOT declared as an ``ActivityExecutor`` subclass -- the protocol
    is structural, so any object with a matching ``execute`` satisfies it, exactly
    as a real worker-facing executor will.
    """

    def __init__(self, results: dict[str, JsonValue]) -> None:
        self._results = results
        self.calls: list[tuple[str, tuple[JsonValue, ...], str | None, int]] = []

    async def execute(
        self,
        name: str,
        args: tuple[JsonValue, ...],
        *,
        workflow_id: str | None,
        seq: int,
    ) -> JsonValue:
        self.calls.append((name, args, workflow_id, seq))
        return self._results[name]


def test_custom_executor_runs_activities_instead_of_a_registry() -> None:
    # The workflow asks for activity "greet"; the executor -- not a registry --
    # supplies the result. This delegation is exactly what reaches a worker
    # process over the wire.
    executor = _RecordingExecutor({"greet": "hello from executor"})
    result = run_sync(_call_activity, ("greet", "world"), InMemoryEventLog(), executor=executor)
    assert result == "hello from executor"
    assert executor.calls == [("greet", ("world",), None, 0)]


def test_executor_receives_workflow_id_and_increasing_seq() -> None:
    # seq is the command's log position (the determinism cursor) and workflow_id
    # identifies the run. Both are what the engine mints an idempotency key from,
    # so a remote executor must receive them verbatim, in order.
    async def two(ctx: WorkflowContext) -> list[JsonValue]:
        return [await ctx.activity("a"), await ctx.activity("b")]

    executor = _RecordingExecutor({"a": 1, "b": 2})
    run_sync(two, (), InMemoryEventLog(), executor=executor, workflow_id="wf-7")
    assert executor.calls == [("a", (), "wf-7", 0), ("b", (), "wf-7", 1)]


def test_passing_both_registry_and_executor_raises() -> None:
    # The two paths are mutually exclusive: in-process (registry) vs distributed
    # (executor). Handing both is a programmer error, caught up front.
    executor = _RecordingExecutor({})
    with pytest.raises(ValueError):
        run_sync(
            _call_activity,
            ("greet",),
            InMemoryEventLog(),
            {"greet": _succeed},
            executor=executor,
        )
