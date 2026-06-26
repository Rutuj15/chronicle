"""Chronicle's event-log vocabulary: Commands and Events.

A workflow is a pure function of its definition, its inputs, and the recorded
event log. Two object families make that concrete:

* **Commands** -- the workflow's *intent*, yielded OUT of the coroutine
  ("please run activity ``greet`` with arg ``world``", or "tell me the time").
  They describe what the workflow wants; they say nothing about outcomes.
* **Events** -- what the runtime *recorded* in the append-only history
  ("activity ``greet`` completed with ``hello world``"). An event answers a
  command by pairing it with its outcome.

Commands and events are deliberately separate types even though they pair 1:1
in Week 1: a command is a question, an event is the recorded answer. Conflating
them would erase the line between "what the workflow asked for" and "what
actually happened" -- the very line the determinism guard checks.
"""

from dataclasses import dataclass

# --- JSON value types --------------------------------------------------------
# Activity results (and the args passed to activities) must be JSON-serializable
# from day one, so Week 2's SQLite persistence is a drop-in rather than a
# redesign. We express "JSON-shaped" precisely -- with Python 3.12's ``type``
# statement -- so mypy --strict can enforce it everywhere a value crosses the
# workflow/runtime boundary.

type JsonScalar = str | int | float | bool | None
type JsonValue = JsonScalar | list[JsonValue] | dict[str, JsonValue]


# --- Commands ----------------------------------------------------------------


@dataclass(frozen=True)
class Command:
    """Base type for everything a workflow yields OUT to the runtime.

    Each concrete command is an *intent* -- a request to perform something
    non-deterministic (run an activity, read the clock). The runtime executes
    it once on first run and feeds the recorded result back on replay.

    Subclasses are frozen dataclasses, so commands compare by value. The
    determinism guard relies on that: it compares a freshly-yielded command
    against the recorded one and raises on any mismatch.
    """


@dataclass(frozen=True)
class ActivityCommand(Command):
    """Intent: run a registered activity by name with positional args.

    Activities are keyed by *string name*, not by function reference, so a
    command stays JSON-serializable and can travel across processes once
    distributed workers land in Week 5.
    """

    name: str
    args: tuple[JsonValue, ...]


@dataclass(frozen=True)
class NowCommand(Command):
    """Intent: read the wall clock.

    Even reading the time is non-deterministic (two runs see different clocks),
    so it must be intercepted, recorded, and replayed. This is the smallest
    teaching example of the core rule: no non-deterministic operation happens
    directly inside workflow code.
    """


@dataclass(frozen=True)
class SleepCommand(Command):
    """Intent: suspend the workflow for a fixed ``duration`` of wall-clock time.

    This is the first command whose resolution is *deferred in real time*: a
    workflow that issues it must not resume until that much time has actually
    passed.

    Only the ``duration`` lives here, never the absolute deadline. The duration
    is deterministic intent -- two runs of the same workflow ask to sleep the
    same number of seconds -- so the determinism guard can replay it. The
    *deadline* (``now() + duration``) depends on when the run started, so it is
    non-deterministic and is recorded only in the matching :class:`TimerFired`
    event, never compared. This is the same intent/outcome split as activity
    args vs. result, applied to time.
    """

    duration: float


# --- Events ------------------------------------------------------------------


@dataclass(frozen=True)
class Event:
    """Base type for a recorded entry in the history.

    Every event remembers the command it answers, so the determinism guard can
    compare a freshly-yielded command against the recorded one (intent vs
    intent -- never result).
    """

    command: Command


@dataclass(frozen=True)
class Completed(Event):
    """An activity finished successfully; ``result`` is its recorded return value.

    ``result`` may legitimately be ``None`` (an activity that returns nothing);
    ``None`` is a valid JSON value, distinct from "no result recorded".
    """

    result: JsonValue


@dataclass(frozen=True)
class Failed(Event):
    """An activity raised -- recorded honestly, *not* retried.

    Reserved so the event schema is stable from the start. Week 1 workflows are
    success-only: an activity that raises aborts the run and we record this.
    Week 4 layers retry/timeout policies on top of this same event type.
    """

    error_type: str
    error_message: str


@dataclass(frozen=True)
class TimerFired(Event):
    """A durable timer fired; ``deadline`` is the absolute instant it was due.

    The runtime stamps ``deadline = now() + duration`` the moment a
    :class:`SleepCommand` arrives, and records this event *before* it starts
    waiting. That ordering is the durability boundary for timers: a crash
    mid-sleep leaves the deadline on disk, so on resume the runtime waits only
    ``deadline - now()`` -- the remainder, not the whole duration again.

    ``deadline`` is a Unix-epoch float (the same clock :class:`NowCommand`
    reads) so a workflow can reason about when its timer was scheduled relative
    to other clock reads. It is also the value fed back to the workflow:
    ``await ctx.sleep(d)`` resolves to this deadline.
    """

    deadline: float


__all__ = [
    "ActivityCommand",
    "Command",
    "Completed",
    "Event",
    "Failed",
    "JsonScalar",
    "JsonValue",
    "NowCommand",
    "SleepCommand",
    "TimerFired",
]
