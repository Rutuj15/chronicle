"""Week 1 definition-of-done tests: deterministic replay with no re-execution.

A workflow runs once (activities execute + events are recorded), then replays
over the same log with activities NOT re-run and an identical result. That gap
between the two runs *is* durable execution (CLAUDE.md §2, §5). These tests pin
every claim behind it: the three run modes, the determinism guard in both
directions, the clock as a recorded command, and honest failure handling.
"""

import pytest

from chronicle.context import WorkflowContext
from chronicle.events import Failed, JsonValue
from chronicle.runtime import (
    ActivityFailedError,
    ActivityRegistry,
    InMemoryEventLog,
    NonDeterminismError,
    run,
)

# --- Shared test inputs ------------------------------------------------------


def _counting_registry() -> tuple[ActivityRegistry, dict[str, int]]:
    """A tiny activity set that counts how many times each runs.

    The counters are the assertion mechanism for replay: if replay re-executed
    an activity, its counter would climb above zero.
    """
    calls: dict[str, int] = {"greet": 0, "shout": 0}

    def greet(name: str) -> str:
        calls["greet"] += 1
        return f"hello {name}"

    def shout(text: str) -> str:
        calls["shout"] += 1
        return text.upper()

    return {"greet": greet, "shout": shout}, calls


async def two_step(ctx: WorkflowContext, name: str) -> str:
    """Canonical demo: greet, then shout the greeting."""
    greeting = await ctx.activity("greet", name)
    shouted = await ctx.activity("shout", greeting)
    return f"{greeting} >>> {shouted}"


async def one_step(ctx: WorkflowContext, name: str) -> str:
    """Stops after a single activity -- exercises the early-return guard."""
    return await ctx.activity("greet", name)


async def read_clock(ctx: WorkflowContext) -> JsonValue:
    """Reads the wall clock once via ctx.now()."""
    return await ctx.now()


async def calls_failing(ctx: WorkflowContext) -> JsonValue:
    """Invokes an activity that always raises."""
    return await ctx.activity("boom")


# --- The three run modes -----------------------------------------------------


def test_first_run_executes_and_records() -> None:
    registry, calls = _counting_registry()
    log = InMemoryEventLog()

    result = run(two_step, ("world",), log, registry)

    assert result == "hello world >>> HELLO WORLD"
    assert calls == {"greet": 1, "shout": 1}
    assert len(log) == 2


def test_replay_does_not_re_execute() -> None:
    registry, calls = _counting_registry()
    log = InMemoryEventLog()

    first = run(two_step, ("world",), log, registry)
    calls["greet"] = calls["shout"] = 0  # reset: replay must not touch activities

    replayed = run(two_step, ("world",), log, registry)

    assert replayed == first
    assert calls == {"greet": 0, "shout": 0}  # the headline: nothing re-ran
    assert len(log) == 2  # pure replay appends nothing


def test_resume_replays_prefix_then_executes() -> None:
    registry, calls = _counting_registry()
    full = InMemoryEventLog()
    run(two_step, ("world",), full, registry)  # record the complete history

    prefix = InMemoryEventLog()
    prefix.append(full[0])  # simulate a crash after only 'greet' was recorded
    calls["greet"] = calls["shout"] = 0

    resumed = run(two_step, ("world",), prefix, registry)

    assert resumed == "hello world >>> HELLO WORLD"
    assert calls == {"greet": 0, "shout": 1}  # greet replayed, shout executed
    assert len(prefix) == 2  # the missing event was recorded on resume


# --- Determinism guard (both directions) -------------------------------------


def test_guard_rejects_divergent_command() -> None:
    registry, _ = _counting_registry()
    log = InMemoryEventLog()
    run(two_step, ("world",), log, registry)  # recorded history

    # Replaying with a different input yields a different first command, so the
    # determinism guard fires instead of feeding back a stale result.
    with pytest.raises(NonDeterminismError):
        run(two_step, ("earth",), log, registry)


def test_guard_rejects_workflow_finishing_early() -> None:
    registry, _ = _counting_registry()
    log = InMemoryEventLog()
    run(two_step, ("world",), log, registry)  # two commands recorded

    # A workflow that stops one command short has diverged from its history.
    with pytest.raises(NonDeterminismError):
        run(one_step, ("world",), log, registry)


# --- Clock and failure handling ----------------------------------------------


def test_now_is_recorded_not_reread() -> None:
    log = InMemoryEventLog()  # no activities needed; ctx.now() is the only command

    first = run(read_clock, (), log, {})
    replayed = run(read_clock, (), log, {})

    assert replayed == first  # the recorded instant, not a fresh clock read
    assert isinstance(first, float)  # JSON scalar (Unix float), never a datetime


def test_activity_failure_is_recorded_and_reproduced() -> None:
    def boom() -> JsonValue:
        raise ValueError("kaboom")

    registry = {"boom": boom}
    log = InMemoryEventLog()

    # First run: the activity raises -> recorded as a Failed event, then aborted.
    with pytest.raises(ActivityFailedError):
        run(calls_failing, (), log, registry)
    assert len(log) == 1
    assert isinstance(log[0], Failed)

    # Replay reproduces the same failure instead of silently succeeding.
    with pytest.raises(ActivityFailedError):
        run(calls_failing, (), log, registry)
