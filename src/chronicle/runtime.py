"""The replay/driver loop, the event-log seam, and the determinism guard.

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
"""

import time
from collections.abc import Callable, Coroutine, Mapping
from typing import Any, Protocol, cast

from .context import WorkflowContext
from .events import (
    ActivityCommand,
    Command,
    Completed,
    Event,
    Failed,
    JsonValue,
    NowCommand,
)

# An activity is plain side-effectful code: takes JSON args, returns a JSON
# value. It runs once per execution and is never replayed (CLAUDE.md §2).
Activity = Callable[..., JsonValue]
ActivityRegistry = Mapping[str, Activity]


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


# --- Event-log seam ----------------------------------------------------------


class EventLog(Protocol):
    """Append-only event history -- the persistence seam.

    The driver loop talks only to this interface, never to a concrete store.
    Week 1 has one implementation (``InMemoryEventLog``); Week 2 adds a
    SQLite-backed store behind this same interface, so swapping storage touches
    one module and leaves the loop untouched (CLAUDE.md §7).
    """

    def append(self, event: Event) -> None: ...

    def __len__(self) -> int: ...

    def __getitem__(self, index: int) -> Event: ...


class InMemoryEventLog(EventLog):
    """A ``list``-backed event log -- Week 1's only store."""

    def __init__(self) -> None:
        self._events: list[Event] = []

    def append(self, event: Event) -> None:
        self._events.append(event)

    def __len__(self) -> int:
        return len(self._events)

    def __getitem__(self, index: int) -> Event:
        return self._events[index]


# --- Internals ---------------------------------------------------------------


def _execute(command: Command, registry: ActivityRegistry) -> Event:
    """Run a command for real (first run only) and wrap its outcome in an Event.

    All side effects live here: looking up + calling the activity, or reading the
    wall clock. On success the activity's result is recorded; on failure we
    record a ``Failed`` event (and the loop re-raises it to abort).
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
            return Completed(command=command, result=time.time())
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


def _outcome(event: Event) -> JsonValue:
    """Resolve a recorded event into what the workflow should receive.

    Success -> the recorded result (fed back into the coroutine). Failure ->
    re-raise, so a recorded crash is reproduced on replay instead of silently
    turning into a success.
    """
    match event:
        case Completed():
            return event.result
        case Failed():
            raise ActivityFailedError(event.error_type, event.error_message)
        case _:
            raise AssertionError(f"unknown event type: {type(event).__name__}")


# --- Public API --------------------------------------------------------------


def run[R](
    workflow: Callable[..., Coroutine[Any, Any, R]],
    args: tuple[JsonValue, ...],
    log: EventLog,
    registry: ActivityRegistry,
) -> R:
    """Drive ``workflow`` to completion over ``log``, deterministically.

    Creates a fresh coroutine, feeds it recorded results for every command it has
    seen before, and executes + records anything new. Returns the workflow's
    final value. The same call serves first run, pure replay, and crash-resume.
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
            event = _execute(command, registry)  # NEW GROUND: execute & record
            log.append(event)
        value_to_send = _outcome(event)
        i += 1


__all__ = [
    "Activity",
    "ActivityFailedError",
    "ActivityRegistry",
    "EventLog",
    "InMemoryEventLog",
    "NonDeterminismError",
    "run",
]
