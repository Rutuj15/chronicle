"""Week 4 (part 1): activity retry policies.

Activities are at-least-once and may fail transiently. A RetryPolicy retries an
activity before recording a terminal failure. These tests pin the contract:
retry-then-succeed records a single Completed; exhaustion records Failed and
raises; pure replay never re-executes or re-retries; backoff uses the injected
sleep so it is fast and assertable; and a bare callable keeps the default
no-retry behavior (CLAUDE.md §4, §8).
"""

import pytest

from chronicle.context import WorkflowContext
from chronicle.events import Completed, Failed, JsonValue
from chronicle.history import InMemoryEventLog
from chronicle.retry import RetryPolicy
from chronicle.runtime import ActivityFailedError, ActivitySpec
from conftest import FakeClock, noop_sleep, run_sync


async def call_activity(ctx: WorkflowContext, name: str) -> JsonValue:
    """Run a single activity by name -- the smallest retry-exercising workflow."""
    return await ctx.activity(name)


def _flaky_registry(
    fail_first: int, retry: RetryPolicy
) -> tuple[dict[str, ActivitySpec], dict[str, int]]:
    """An activity that fails its first ``fail_first`` calls, then succeeds.

    Returns the registry and a shared call counter, so tests can assert exactly
    how many attempts the retry loop made.
    """
    calls: dict[str, int] = {"flaky": 0}

    async def flaky() -> str:
        calls["flaky"] += 1
        if calls["flaky"] <= fail_first:
            raise RuntimeError(f"transient #{calls['flaky']}")
        return "recovered"

    return {"flaky": ActivitySpec(flaky, retry=retry)}, calls


# --- RetryPolicy unit tests --------------------------------------------------


def test_backoff_schedule_is_exponential_and_capped() -> None:
    policy = RetryPolicy(max_attempts=5, initial_backoff=1.0, backoff_factor=2.0, max_backoff=5.0)
    assert policy.backoff_for(1) == 1.0
    assert policy.backoff_for(2) == 2.0
    assert policy.backoff_for(3) == 4.0
    assert policy.backoff_for(4) == 5.0  # 8.0 capped at max_backoff


def test_zero_initial_backoff_retries_immediately() -> None:
    policy = RetryPolicy(max_attempts=3, initial_backoff=0.0)
    assert policy.backoff_for(1) == 0.0
    assert policy.backoff_for(2) == 0.0


def test_invalid_policy_is_rejected() -> None:
    with pytest.raises(ValueError):
        RetryPolicy(max_attempts=0)


# --- Retry behavior ----------------------------------------------------------


def test_retry_succeeds_on_later_attempt() -> None:
    policy = RetryPolicy(max_attempts=3, initial_backoff=1.0, backoff_factor=2.0)
    registry, calls = _flaky_registry(fail_first=2, retry=policy)  # fails twice, wins 3rd
    log = InMemoryEventLog()

    result = run_sync(call_activity, ("flaky",), log, registry, sleep=noop_sleep)

    assert result == "recovered"
    assert calls["flaky"] == 3  # failed twice, succeeded on the third and final attempt
    assert len(log) == 1
    assert isinstance(log[0], Completed)  # one terminal event -- no per-attempt noise


def test_retry_exhaustion_records_failed_and_raises() -> None:
    calls: dict[str, int] = {"boom": 0}

    async def boom() -> JsonValue:
        calls["boom"] += 1
        raise RuntimeError("nope")

    registry = {"boom": ActivitySpec(boom, retry=RetryPolicy(max_attempts=3))}
    log = InMemoryEventLog()

    with pytest.raises(ActivityFailedError):
        run_sync(call_activity, ("boom",), log, registry, sleep=noop_sleep)

    assert calls["boom"] == 3  # tried up to max_attempts, then gave up
    assert len(log) == 1
    assert isinstance(log[0], Failed)


def test_replay_does_not_re_execute_or_retry() -> None:
    policy = RetryPolicy(max_attempts=3, initial_backoff=1.0, backoff_factor=2.0)
    registry, calls = _flaky_registry(fail_first=2, retry=policy)
    log = InMemoryEventLog()
    run_sync(call_activity, ("flaky",), log, registry, sleep=noop_sleep)  # records Completed
    calls["flaky"] = 0  # reset: replay must not touch the activity at all

    result = run_sync(call_activity, ("flaky",), log, registry, sleep=noop_sleep)

    assert result == "recovered"
    assert calls["flaky"] == 0  # pure replay executes zero activities -> zero retries
    assert len(log) == 1  # and appends nothing


def test_backoff_waits_use_the_injected_sleep() -> None:
    policy = RetryPolicy(max_attempts=3, initial_backoff=1.0, backoff_factor=2.0)
    registry, _calls = _flaky_registry(fail_first=2, retry=policy)
    clock = FakeClock()

    run_sync(call_activity, ("flaky",), InMemoryEventLog(), registry, sleep=clock.sleep)

    # After attempt 1 fails -> backoff_for(1); after attempt 2 -> backoff_for(2).
    assert clock.waits == [policy.backoff_for(1), policy.backoff_for(2)]
    assert clock.waits == [1.0, 2.0]


def test_bare_callable_has_no_retry_by_default() -> None:
    calls: dict[str, int] = {"once": 0}

    async def once() -> JsonValue:
        calls["once"] += 1
        raise RuntimeError("fail")

    log = InMemoryEventLog()
    with pytest.raises(ActivityFailedError):
        run_sync(call_activity, ("once",), log, {"once": once}, sleep=noop_sleep)
    assert calls["once"] == 1  # default policy: a single attempt, no retry
