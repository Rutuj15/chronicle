"""Week 3 definition-of-done tests: durable timers survive a mid-sleep crash.

A workflow that suspends on a timer records the absolute deadline *before* it
waits. On resume -- even after the runtime is discarded -- the workflow replays
up to that timer and then waits only the *remainder* of the original deadline,
not the whole duration again. That remainder-respect across a discard is durable
timers made real (CLAUDE.md §2, §4).

Every test injects a fake clock so no real wall-clock time is spent: ``now`` is
a controllable float and ``sleep`` advances it (and records the wait) instead of
blocking. That is exactly what makes the remainder math assertable.
"""

import pytest

from chronicle.context import WorkflowContext
from chronicle.events import JsonValue, SleepCommand, TimerFired
from chronicle.history import InMemoryEventLog
from chronicle.runtime import ActivityRegistry, NonDeterminismError
from conftest import FakeClock, noop_sleep, run_sync

# --- the workflows under test ------------------------------------------------


async def sleep_once(ctx: WorkflowContext, duration: float) -> JsonValue:
    """Sleep once and return the deadline the runtime fed back."""
    return await ctx.sleep(duration)


async def sleep_between_activities(ctx: WorkflowContext) -> JsonValue:
    """An activity, then a sleep, then another activity -- the flow suspends mid-way."""
    before = await ctx.activity("stamp", "before")
    deadline = await ctx.sleep(5.0)
    after = await ctx.activity("stamp", "after")
    return {"before": before, "deadline": deadline, "after": after}


# --- first run records a timer and waits the full duration --------------------


def test_first_run_records_timer_and_waits_the_duration() -> None:
    clock = FakeClock(1000.0)
    log = InMemoryEventLog()

    deadline = run_sync(sleep_once, (10.0,), log, {}, now=clock.now, sleep=clock.sleep)

    assert deadline == 1010.0  # now() + duration, fed back to the workflow
    assert clock.waits == [10.0]  # waited the full duration on first run
    assert clock.time == 1010.0  # the fake clock advanced by the wait
    assert len(log) == 1
    event = log[0]
    assert isinstance(event, TimerFired)
    assert event.deadline == 1010.0
    assert isinstance(event.command, SleepCommand)
    assert event.command.duration == 10.0


# --- pure replay of a fired timer never blocks --------------------------------


def test_replay_of_completed_timer_waits_nothing() -> None:
    # Record once, starting the clock at 1000 -> the deadline becomes 1010.
    recorded = InMemoryEventLog()
    run_sync(
        sleep_once,
        (10.0,),
        recorded,
        {},
        now=FakeClock(1000.0).now,
        sleep=noop_sleep,
    )

    # Replay well after the deadline: the recorded deadline is now in the past.
    later = FakeClock(5000.0)
    result = run_sync(sleep_once, (10.0,), recorded, {}, now=later.now, sleep=later.sleep)

    assert result == 1010.0  # the recorded deadline, fed back unchanged
    assert later.waits == []  # pure replay of a fired timer never blocks
    assert len(recorded) == 1  # nothing new is appended on replay


# --- the headline: resume after a crash waits only the remainder --------------


def test_resume_after_crash_waits_only_the_remainder() -> None:
    # Process 1 records the timer (deadline 1010) and then "dies" mid-sleep.
    # Only the recorded history crosses the process boundary -- no runtime state.
    recorded = InMemoryEventLog()
    run_sync(
        sleep_once,
        (10.0,),
        recorded,
        {},
        now=FakeClock(1000.0).now,
        sleep=noop_sleep,  # record without actually waiting
    )
    assert isinstance(recorded[0], TimerFired)
    assert recorded[0].deadline == 1010.0

    # Process 2 restarts at clock=1005 -- 5s short of the recorded deadline.
    process2 = FakeClock(1005.0)
    result = run_sync(sleep_once, (10.0,), recorded, {}, now=process2.now, sleep=process2.sleep)

    assert result == 1010.0
    assert process2.waits == [5.0]  # the remainder, NOT the full 10s again
    assert process2.time == 1010.0  # advanced only by the remainder
    assert len(recorded) == 1  # nothing is re-recorded on resume


# --- the determinism guard applies to the duration (intent), not the deadline -


def test_guard_rejects_a_divergent_duration() -> None:
    recorded = InMemoryEventLog()
    run_sync(
        sleep_once,
        (10.0,),
        recorded,
        {},
        now=FakeClock(1000.0).now,
        sleep=noop_sleep,
    )

    # A different duration issues a different SleepCommand at the same position.
    with pytest.raises(NonDeterminismError):
        run_sync(
            sleep_once,
            (20.0,),
            recorded,
            {},
            now=FakeClock(1000.0).now,
            sleep=noop_sleep,
        )


# --- a timer mid-flow suspends the whole orchestration ------------------------


def test_timer_between_activities_suspends_then_resumes() -> None:
    calls: list[str] = []

    async def stamp(label: str) -> str:
        calls.append(label)
        return label

    registry: ActivityRegistry = {"stamp": stamp}
    clock = FakeClock(0.0)
    log = InMemoryEventLog()

    result = run_sync(sleep_between_activities, (), log, registry, now=clock.now, sleep=clock.sleep)

    assert result == {"before": "before", "deadline": 5.0, "after": "after"}
    assert calls == ["before", "after"]  # both activities ran, in order
    assert clock.waits == [5.0]  # exactly one wait, between the two activities
    assert len(log) == 3  # activity, timer, activity -- recorded in order


# --- edge: a non-positive duration is a no-op timer ---------------------------


def test_zero_duration_timer_does_not_wait() -> None:
    clock = FakeClock(1000.0)
    log = InMemoryEventLog()

    deadline = run_sync(sleep_once, (0.0,), log, {}, now=clock.now, sleep=clock.sleep)

    assert deadline == 1000.0  # deadline == now()
    assert clock.waits == []  # remaining == 0 -> the guard `> 0` skips the wait
    assert isinstance(log[0], TimerFired)
    assert log[0].deadline == 1000.0
