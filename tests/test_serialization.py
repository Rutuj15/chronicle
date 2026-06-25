"""Round-trip tests for the event-log serialization envelope.

These pin the two properties the durable store will depend on (serialization.py
docstring): type preservation across the Event/Command tagged union, and tuple
fidelity for ``ActivityCommand.args`` -- without which the determinism guard
would falsely fire on every durable replay.
"""

import json

import pytest

from chronicle.context import WorkflowContext
from chronicle.events import (
    ActivityCommand,
    Completed,
    Event,
    Failed,
    JsonValue,
    NowCommand,
    SleepCommand,
    TimerFired,
)
from chronicle.history import InMemoryEventLog
from chronicle.runtime import ActivityRegistry
from chronicle.serialization import dump_event, load_event
from conftest import run_sync

# --- the union: every variant round-trips ------------------------------------


@pytest.mark.parametrize(
    "event",
    [
        Completed(ActivityCommand("greet", ("world",)), "hello world"),
        Completed(NowCommand(), 1_700_000_000.5),
        Failed(ActivityCommand("boom", ()), "ValueError", "kaboom"),
        Failed(NowCommand(), "RuntimeError", "clock broke"),
        TimerFired(SleepCommand(10.0), 1_700_000_010.5),
    ],
    ids=[
        "completed-activity",
        "completed-now",
        "failed-activity",
        "failed-now",
        "timer-fired-sleep",
    ],
)
def test_round_trip_preserves_event(event: Event) -> None:
    loaded = load_event(dump_event(event))

    assert loaded == event  # value-equal ...
    assert type(loaded) is type(event)  # ... and the exact subclass reconstructed


def test_activity_args_round_trip_as_tuple() -> None:
    # The whole reason tuple coercion exists: a list would make the guard fire.
    command = ActivityCommand("greet", ("world", 42, True, None))
    loaded = load_event(dump_event(Completed(command, None))).command

    assert isinstance(loaded.args, tuple)
    assert loaded == command


@pytest.mark.parametrize(
    "result",
    [
        None,
        True,
        42,
        3.14,
        "text",
        [1, "two", [3, {"four": 4}], None],
        {"nested": {"list": [1, 2], "flag": False}},
        {"empty_list": [], "empty_dict": {}},
    ],
)
def test_json_results_round_trip(result: JsonValue) -> None:
    event = Completed(ActivityCommand("op", ()), result)
    assert load_event(dump_event(event)).result == result


def test_envelope_shape_is_versioned_and_tagged() -> None:
    # Pins the on-disk wire format so it is documented and stable.
    payload = dump_event(Completed(ActivityCommand("greet", ("world",)), "hi"))

    assert json.loads(payload) == {
        "v": 1,
        "kind": "completed",
        "command": {"kind": "activity", "name": "greet", "args": ["world"]},
        "result": "hi",
    }


def test_timer_fired_envelope_shape() -> None:
    # The timer event persists the deadline (outcome), not just the duration
    # (intent) -- so on reopen the remainder can be recomputed. Pin its shape.
    payload = dump_event(TimerFired(SleepCommand(10.0), 1010.5))

    assert json.loads(payload) == {
        "v": 1,
        "kind": "timer_fired",
        "command": {"kind": "sleep", "duration": 10.0},
        "deadline": 1010.5,
    }


# --- robustness against a corrupt / future log -------------------------------


def test_unknown_envelope_version_is_rejected() -> None:
    payload = json.dumps({"v": 999, "kind": "completed", "command": {"kind": "now"}, "result": 0})
    with pytest.raises(ValueError, match="version"):
        load_event(payload)


def test_unknown_event_kind_is_rejected() -> None:
    payload = json.dumps({"v": 1, "kind": "mystery", "command": {"kind": "now"}, "result": 0})
    with pytest.raises(ValueError, match="kind"):
        load_event(payload)


# --- the contract that matters: a serialized log replays via the real engine --


def test_serialized_log_replays_with_no_activity_re_execution() -> None:
    """A log dumped to bytes and loaded back must replay identically, with no
    activity re-execution -- exactly what the SQLite store will rely on.

    If decoding left ``ActivityCommand.args`` as a list, the determinism guard
    would raise right here, so this also guards the tuple-coercion invariant in
    the one place that actually exercises it.
    """
    calls = {"greet": 0, "shout": 0}

    async def greet(name: str) -> str:
        calls["greet"] += 1
        return f"hello {name}"

    async def shout(text: str) -> str:
        calls["shout"] += 1
        return text.upper()

    registry: ActivityRegistry = {"greet": greet, "shout": shout}

    async def two_step(ctx: WorkflowContext, name: str) -> str:
        greeting = await ctx.activity("greet", name)
        shouted = await ctx.activity("shout", greeting)
        return f"{greeting} >>> {shouted}"

    original = InMemoryEventLog()
    run_sync(two_step, ("world",), original, registry)
    calls["greet"] = calls["shout"] = 0  # reset: replay must not touch activities

    # Simulate the durable store: every event leaves the process as bytes and
    # comes back as a reconstructed object.
    rebuilt = InMemoryEventLog()
    for i in range(len(original)):
        rebuilt.append(load_event(dump_event(original[i])))

    result = run_sync(two_step, ("world",), rebuilt, registry)

    assert result == "hello world >>> HELLO WORLD"
    assert calls == {"greet": 0, "shout": 0}  # nothing re-ran despite the byte detour
