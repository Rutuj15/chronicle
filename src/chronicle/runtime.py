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
from dataclasses import dataclass, field
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
from .retry import RetryPolicy, idempotency_key

# An activity is plain side-effectful code: takes JSON args, returns a JSON
# value. It runs once per execution and is never replayed (CLAUDE.md §2).
Activity = Callable[..., JsonValue]


@dataclass(frozen=True)
class ActivitySpec:
    """An activity bound to its execution policies.

    Activities are registered by name alongside the policies that govern how the
    runtime runs them: ``retry`` and ``idempotent`` (both Week 4). When
    ``idempotent`` is set the runtime injects a stable ``idempotency_key``
    keyword arg into each call so the activity can dedup across the
    at-least-once boundary; a per-attempt ``timeout`` knob joins here in Week 5
    with async activities. A bare callable may be registered in place of a spec
    -- it is normalized to a spec with the defaults (no retry, not idempotent)
    in :func:`run` (see :func:`_normalize_registry`).
    """

    fn: Activity
    retry: RetryPolicy = field(default_factory=RetryPolicy)
    idempotent: bool = False


# The registry a caller hands to ``run``: activity name -> either a bare
# callable (default policy) or a full ActivitySpec. Normalized to specs inside
# ``run`` so the rest of the runtime always sees a spec.
ActivityRegistry = Mapping[str, Activity | ActivitySpec]

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

    The activity was retried up to its policy's ``max_attempts`` and still
    failed, so the runtime records a ``Failed`` event and aborts by raising
    this. On replay, a recorded failure is reproduced by re-raising this same
    error, so a crash stays a crash.
    """

    def __init__(self, error_type: str, error_message: str) -> None:
        super().__init__(f"{error_type}: {error_message}")
        self.error_type = error_type
        self.error_message = error_message


# --- Internals ---------------------------------------------------------------


def _execute(
    command: Command,
    registry: Mapping[str, ActivitySpec],
    *,
    now: Clock,
    sleep: Sleeper,
    workflow_id: str | None,
    seq: int,
) -> Event:
    """Run a command for real (first run only) and wrap its outcome in an Event.

    All side effects live here: looking up + running the activity under its
    retry policy, reading the injected clock, or stamping a timer's deadline.
    On success the activity's result is recorded; on failure -- after the retry
    policy is exhausted -- we record a ``Failed`` event (and the loop re-raises
    it to abort).

    A ``SleepCommand`` is recorded but NOT waited for here -- it only stamps the
    deadline. The actual wait lives in :func:`_resolve`, which is shared by the
    first-run and replay branches, because a timer resumed mid-sleep must wait
    its remainder on the *replay* path that ``_execute`` never sees
    (CLAUDE.md §4, Week 3).
    """
    match command:
        case ActivityCommand(name, args):
            spec = _require_activity(registry, name)
            key: str | None
            if spec.idempotent:
                # An idempotent activity needs a key; the key needs the run's
                # identity. Non-idempotent activities never mint one, so
                # workflow_id stays optional for them (CLAUDE.md §8, Week 4).
                if workflow_id is None:
                    raise ValueError(
                        f"activity {name!r} is registered idempotent, so run() "
                        f"needs a workflow_id to mint its idempotency key"
                    )
                key = idempotency_key(workflow_id, seq)
            else:
                key = None
            try:
                # _run_activity retries per the spec's policy and injects the
                # idempotency key (stable across attempts); on exhaustion it
                # re-raises so we record a single Failed event for the whole
                # attempt sequence (CLAUDE.md §4, Week 4).
                result = _run_activity(spec, args, key=key, sleep=sleep)
            except Exception as exc:  # any terminal failure becomes a recorded Failed event
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


def _normalize_registry(registry: ActivityRegistry) -> dict[str, ActivitySpec]:
    """Accept bare callables OR ActivitySpecs; return specs throughout.

    Existing callers register plain functions (``{"greet": greet}``); Week 4
    adds ``ActivitySpec`` to attach a retry policy. Normalizing once, here, lets
    :func:`_execute` assume it always has a spec, so the bare-callable default
    (no retry) keeps working without touching every call site.
    """
    normalized: dict[str, ActivitySpec] = {}
    for name, entry in registry.items():
        normalized[name] = entry if isinstance(entry, ActivitySpec) else ActivitySpec(fn=entry)
    return normalized


def _require_activity(registry: Mapping[str, ActivitySpec], name: str) -> ActivitySpec:
    """Look up an activity by name, or raise.

    A missing activity is a setup bug, not a runtime failure -- it must not be
    swallowed into a Failed event, so it raises ``KeyError`` before any retry
    logic runs.
    """
    if name not in registry:
        raise KeyError(f"no activity registered as {name!r}")
    return registry[name]


def _run_activity(
    spec: ActivitySpec,
    args: tuple[JsonValue, ...],
    *,
    key: str | None,
    sleep: Sleeper,
) -> JsonValue:
    """Call ``spec.fn`` under its retry policy, returning the result.

    Retries on any ``Exception`` up to ``spec.retry.max_attempts`` times,
    waiting the policy's backoff between attempts via the injected ``sleep``
    (a plain wait -- NOT a recorded ``SleepCommand`` -- so retries leave no
    trace in the event log). ``BaseException`` is never caught, so
    ``KeyboardInterrupt`` / ``SystemExit`` propagate untouched. When every
    attempt fails, the last exception propagates to :func:`_execute`, which
    records a single ``Failed`` event for the whole sequence.

    ``key`` is the idempotency key injected as ``idempotency_key=`` when the
    spec is idempotent (``None`` otherwise). It is built once, before the loop,
    so every retry of the same invocation presents the *same* key -- a retry
    re-runs the same activity, not a new one.

    This runs only on first run / new ground: pure replay never calls it, so
    retries, backoff waits, and key injection never happen on replay
    (CLAUDE.md §4, W4).
    """
    policy = spec.retry
    # Same key on every attempt: a retry re-runs the SAME invocation, so it must
    # show the downstream system the SAME key (CLAUDE.md §4, Week 4).
    kwargs: dict[str, str] = {} if key is None else {"idempotency_key": key}
    attempt = 0
    while True:
        attempt += 1
        try:
            return spec.fn(*args, **kwargs)
        except Exception:
            if attempt >= policy.max_attempts:
                raise
            sleep(policy.backoff_for(attempt))


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
    workflow_id: str | None = None,
    now: Clock = time.time,
    sleep: Sleeper = time.sleep,
) -> R:
    """Drive ``workflow`` to completion over ``log``, deterministically.

    Creates a fresh coroutine, feeds it recorded results for every command it has
    seen before, and executes + records anything new. Returns the workflow's
    final value. The same call serves first run, pure replay, and crash-resume.

    ``now`` and ``sleep`` are the clock a workflow experiences. They default to
    the real OS calls; tests inject fakes so timer and retry behaviour can be
    asserted without real wall-clock waiting. A durable timer is resolved
    (possibly waiting its remainder) inside this loop via :func:`_resolve`;
    ``sleep`` is also used directly for retry backoff in :func:`_execute`.

    ``workflow_id`` identifies this execution. It is optional in general but
    required the moment any registered activity is ``idempotent``: the runtime
    mints each such activity a stable key ``"{workflow_id}:{seq}"`` (see
    :func:`~chronicle.retry.idempotency_key`) so it can dedup across the
    at-least-once boundary. ``seq`` is the command's position in the log -- the
    same on every run, by deterministic replay.
    """
    # Bare callables become specs with the default (no-retry) policy, so a plain
    # {"name": fn} registry keeps working unchanged (CLAUDE.md §8, Week 4).
    specs = _normalize_registry(registry)
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
            event = _execute(
                command,
                specs,
                now=now,
                sleep=sleep,
                workflow_id=workflow_id,
                seq=i,
            )  # NEW GROUND: execute & record
            log.append(event)
        value_to_send = _resolve(event, now=now, sleep=sleep)  # may wait on a timer
        i += 1


__all__ = [
    "Activity",
    "ActivityFailedError",
    "ActivityRegistry",
    "ActivitySpec",
    "Clock",
    "EventLog",
    "NonDeterminismError",
    "Sleeper",
    "run",
]
