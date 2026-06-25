"""Week 5, slice 2: per-attempt activity timeouts.

With async activities (slice 1) a real wall-clock timeout can now *interrupt* an
in-flight call: ``asyncio.wait_for`` cancels the activity's task past the budget
and raises ``TimeoutError``. The timeout lives on ``ActivitySpec`` -- execution
policy, like retry/idempotency -- so it is excluded from the determinism guard
and pure replay never re-times out. A timeout is an ordinary retriable failure:
retried per-attempt under the policy, recorded as one ``Failed`` on exhaustion.

Note: ``asyncio.wait_for`` reads the event loop's own monotonic clock, so these
tests use small *real* wall-clock values (not the injected ``FakeClock``) -- the
first behaviour in the suite tested against real time (CLAUDE.md §8, Week 5).
"""

import asyncio

import pytest

from chronicle.context import WorkflowContext
from chronicle.events import Completed, Failed, JsonValue
from chronicle.history import InMemoryEventLog
from chronicle.retry import RetryPolicy
from chronicle.runtime import ActivityFailedError, ActivitySpec
from conftest import run_sync

# A budget comfortably larger than any activity here, and an overrun ten times
# the budget -- chosen so the timing assertions are robust on a loaded machine.
GENEROUS = 0.5
OVER_BUDGET = 0.05


async def call_activity(ctx: WorkflowContext, name: str) -> JsonValue:
    """Run a single activity by name -- the smallest timeout-exercising workflow."""
    return await ctx.activity(name)


# --- Under / over budget ----------------------------------------------------


def test_activity_under_timeout_succeeds() -> None:
    async def quick() -> str:
        await asyncio.sleep(0.01)  # well inside the budget
        return "ok"

    log = InMemoryEventLog()
    registry = {"quick": ActivitySpec(quick, timeout=GENEROUS)}

    result = run_sync(call_activity, ("quick",), log, registry)

    assert result == "ok"
    assert isinstance(log[0], Completed)


def test_activity_exceeding_timeout_fails() -> None:
    async def slow() -> str:
        await asyncio.sleep(GENEROUS)  # ten times the budget
        return "never"

    log = InMemoryEventLog()

    with pytest.raises(ActivityFailedError):
        run_sync(call_activity, ("slow",), log, {"slow": ActivitySpec(slow, timeout=OVER_BUDGET)})

    assert isinstance(log[0], Failed)
    assert log[0].error_type == "TimeoutError"  # the cancellation, not app logic


# --- Timeout composes with retry -------------------------------------------


def test_timeout_is_retryable_then_succeeds() -> None:
    calls: dict[str, int] = {"flaky": 0}

    async def flaky() -> str:
        calls["flaky"] += 1
        if calls["flaky"] == 1:
            await asyncio.sleep(GENEROUS)  # attempt 1: over budget -> TimeoutError
        return "recovered"  # attempt 2: fast, succeeds

    registry = {
        "flaky": ActivitySpec(flaky, retry=RetryPolicy(max_attempts=3), timeout=OVER_BUDGET),
    }
    log = InMemoryEventLog()

    result = run_sync(call_activity, ("flaky",), log, registry)

    assert result == "recovered"
    assert calls["flaky"] == 2  # timed out once, then succeeded
    assert isinstance(log[0], Completed)  # one terminal event -- no per-attempt noise


def test_timeout_exhaustion_with_retry_records_failed() -> None:
    calls: dict[str, int] = {"slow": 0}

    async def always_slow() -> str:
        calls["slow"] += 1
        await asyncio.sleep(GENEROUS)  # every attempt exceeds the budget
        return "never"

    registry = {
        "slow": ActivitySpec(
            always_slow,
            retry=RetryPolicy(max_attempts=2, initial_backoff=0.0),
            timeout=OVER_BUDGET,
        )
    }
    log = InMemoryEventLog()

    with pytest.raises(ActivityFailedError):
        run_sync(call_activity, ("slow",), log, registry)

    assert calls["slow"] == 2  # tried max_attempts times, each timed out
    assert isinstance(log[0], Failed)
    assert log[0].error_type == "TimeoutError"


# --- Replay is inert --------------------------------------------------------


def test_replay_does_not_re_time_out() -> None:
    calls: dict[str, int] = {"quick": 0}

    async def quick() -> str:
        calls["quick"] += 1
        return "ok"

    log = InMemoryEventLog()
    # Record under a generous budget -> Completed (the activity runs once).
    run_sync(call_activity, ("quick",), log, {"quick": ActivitySpec(quick, timeout=GENEROUS)})
    assert calls["quick"] == 1
    calls["quick"] = 0

    # Replay under a PATHOLOGICAL 0.0s budget: pure replay never calls _execute,
    # so the timeout is never consulted -> the recorded result returns and the
    # activity does not run at all (Week-1 DoD holds: zero executions on replay).
    result = run_sync(call_activity, ("quick",), log, {"quick": ActivitySpec(quick, timeout=0.0)})

    assert result == "ok"
    assert calls["quick"] == 0
    assert len(log) == 1  # nothing appended


# --- Validation -------------------------------------------------------------


def test_negative_timeout_rejected() -> None:
    async def ok() -> JsonValue:
        return "ok"

    with pytest.raises(ValueError):
        ActivitySpec(ok, timeout=-1.0)
