"""The replay/driver loop and the determinism guard.

This is the heart of Chronicle (CLAUDE.md §5). One ``.send()`` loop drives a
workflow coroutine and simultaneously handles three modes:

* **first run** -- the log is empty, so every command is new ground: execute it
  and append the resulting event.
* **pure replay** -- the log already holds the full history, so every command
  matches a recorded event: feed the recorded result back, never re-execute.
* **resume after a crash** -- a known prefix replays, then the loop crosses into
  new ground and executes + records the rest.

Same loop, three modes -- which branch is taken depends only on whether the
cursor ``i`` is still inside the recorded history.

The append-only log this replays over is the ``EventLog`` seam, defined in
``history.py``; ``run`` is indifferent to whether that log is in memory or on
disk -- which is what lets Week 2 swap in SQLite without touching this loop.
"""

import time
from collections.abc import Callable, Coroutine, Mapping
from typing import Any, cast

from .context import WorkflowContext
from .events import (
    ActivityCommand,
    Command,
    Completed,
    Event,
    Failed,
    JsonValue,
    NowCommand,
    SleepCommand,
    TimerFired,
)
from .history import EventLog

# An activity is plain side-effectful code: takes JSON args, returns a JSON
# value. It runs once per execution and is never replayed (CLAUDE.md §2).
Activity = Callable[..., JsonValue]
ActivityRegistry = Mapping[str, Activity]

# The clock a workflow experiences is injected, never read straight from the OS.
# That is what makes timers testable without real wall-clock waiting: a test
# passes a controllable ``now`` and a ``sleep`` that records instead of blocking,
# and can then assert exact remainder math (CLAUDE.md §11). Defaults are the real
# OS calls, so production behaviour is unchanged. One clock source -- wall-clock
# Unix floats, the same one ``NowCommand`` reads -- is used everywhere; swap both
# defaults to monotonic in one place if clock-jump robustness is ever needed.
Clock = Callable[[], float]
Sleeper = Callable[[float], None]


# --- Errors ------------------------------------------------------------------


class NonDeterminismError(RuntimeError):
    """Raised on replay when a command doesn't match the recorded history.

    The workflow issued a different command at this position than it did on the
    recorded run -- either a different command, or the wrong number of them.
    That means the workflow is non-deterministic: it branched on something not
    captured in the event log (wall-clock, randomness, external data read inside
    the workflow). All such operations must be commands.
    """


class ActivityFailedError(RuntimeError):
    """An activity raised during execution (or replay of a recorded failure).

    Week 1 is success-only: we record the failure honestly and abort -- no
    retry, no timeout (those arrive in Week 4). On replay, a recorded failure is
    reproduced by re-raising this same error, so a crash stays a crash.
    """

    def __init__(self, error_type: str, error_message: str) -> None:
        super().__init__(f"{error_type}: {error_message}")
        self.error_type = error_type
        self.error_message = error_message


# --- Internals ---------------------------------------------------------------


def _execute(
    command: Command,
    registry: ActivityRegistry,
    *,
    now: Clock,
) -> Event:
    """Run a command for real (first run only) and wrap its outcome in an Event.

    All side effects live here: looking up + calling the activity, reading the
    injected clock, or stamping a timer's deadline. On success the activity's
    result is recorded; on failure we record a ``Failed`` event (and the loop
    re-raises it to abort).

    A ``SleepCommand`` is recorded but NOT waited for here -- it only stamps the
    deadline. The actual wait lives in :func:`_resolve`, which is shared by the
    first-run and replay branches, because a timer resumed mid-sleep must wait
    its remainder on the *replay* path that ``_execute`` never sees
    (CLAUDE.md §4, Week 3).
    """
    match command:
        case ActivityCommand(name, args):
            if name not in registry:
                # A missing activity is a setup bug, not a runtime failure -- it
                # must not be swallowed into a Failed event.
                raise KeyError(f"no activity registered as {name!r}")
            activity = registry[name]
            try:
                result = activity(*args)
            except Exception as exc:  # any activity failure becomes a recorded Failed event
                return Failed(
                    command=command,
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                )
            return Completed(command=command, result=result)
        case NowCommand():
            # The clock is read exactly once, on first run; replay feeds back the
            # recorded float. Never a datetime -- JSON-native by contract.
            return Completed(command=command, result=now())
        case SleepCommand(duration):
            # Stamp the absolute deadline from the SAME clock NowCommand uses,
            # and record it BEFORE any waiting happens (see _resolve). The wait
            # is deferred so the deadline is already durable when it occurs.
            return TimerFired(command=command, deadline=now() + duration)
        case _:
            raise AssertionError(f"unknown command type: {type(command).__name__}")


def _assert_matches(command: Command, event: Event) -> None:
    """Determinism guard: compare intent, never result.

    The command the workflow just issued must equal the command history says it
    issued at this position. Equality is value-based (frozen dataclasses), so a
    mismatched activity name, args, or command *type* all raise.
    """
    if command != event.command:
        raise NonDeterminismError(
            "non-deterministic workflow: history recorded "
            f"{event.command!r} but replay issued {command!r}"
        )


def _resolve(event: Event, *, now: Clock, sleep: Sleeper) -> JsonValue:
    """Resolve a recorded event into what the workflow should receive.

    Success -> the recorded result (fed back into the coroutine). Failure ->
    re-raise, so a recorded crash is reproduced on replay instead of silently
    turning into a success. Timer -> wait until the recorded deadline if it is
    still in the future, then return that deadline.

    This is the one place replay can block in real time. A timer resumed
    mid-sleep has a future deadline, so resolving it waits the remainder; pure
    replay of an already-completed workflow never blocks, because every recorded
    deadline is in the past by then. The wait is a side effect only -- the
    *result* fed back is the same deadline either way, so determinism holds
    (CLAUDE.md §4, Week 3).
    """
    match event:
        case Completed():
            return event.result
        case Failed():
            raise ActivityFailedError(event.error_type, event.error_message)
        case TimerFired():
            remaining = event.deadline - now()
            if remaining > 0:
                sleep(remaining)
            return event.deadline
        case _:
            raise AssertionError(f"unknown event type: {type(event).__name__}")


# --- Public API --------------------------------------------------------------


def run[R](
    workflow: Callable[..., Coroutine[Any, Any, R]],
    args: tuple[JsonValue, ...],
    log: EventLog,
    registry: ActivityRegistry,
    *,
    now: Clock = time.time,
    sleep: Sleeper = time.sleep,
) -> R:
    """Drive ``workflow`` to completion over ``log``, deterministically.

    Creates a fresh coroutine, feeds it recorded results for every command it has
    seen before, and executes + records anything new. Returns the workflow's
    final value. The same call serves first run, pure replay, and crash-resume.

    ``now`` and ``sleep`` are the clock a workflow experiences. They default to
    the real OS calls; tests inject fakes so timer behaviour can be asserted
    without real wall-clock waiting. A durable timer is resolved (possibly
    waiting its remainder) inside this loop via :func:`_resolve`.
    """
    ctx = WorkflowContext()
    coro = workflow(ctx, *args)
    value_to_send: JsonValue | None = None
    i = 0
    while True:
        try:
            command = coro.send(value_to_send)
        except StopIteration as done:
            # Guard the other direction too: a workflow that returns *fewer*
            # commands than were recorded has diverged from the recorded run.
            if i < len(log):
                raise NonDeterminismError(
                    "non-deterministic workflow: finished after "
                    f"{i} command(s) but {len(log)} were recorded"
                ) from None
            return cast(R, done.value)
        if i < len(log):
            event = log[i]  # REPLAY: seen this command before
            _assert_matches(command, event)
        else:
            event = _execute(command, registry, now=now)  # NEW GROUND: execute & record
            log.append(event)
        value_to_send = _resolve(event, now=now, sleep=sleep)  # may wait on a timer
        i += 1


__all__ = [
    "Activity",
    "ActivityFailedError",
    "ActivityRegistry",
    "Clock",
    "EventLog",
    "NonDeterminismError",
    "Sleeper",
    "run",
]
