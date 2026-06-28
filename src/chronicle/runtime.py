"""The replay/driver loop and the determinism guard.

This is the heart of Chronicle. One ``.send()`` loop drives a
workflow coroutine and simultaneously handles three modes:

* **first run** -- the log is empty, so every command is new ground: execute it
  and append the resulting event.
* **pure replay** -- the log already holds the full history, so every command
  matches a recorded event: feed the recorded result back, never re-execute.
* **resume after a crash** -- a known prefix replays, then the loop crosses into
  new ground and executes + records the rest.

Same loop, three modes -- which branch is taken depends only on whether the
cursor ``i`` is still inside the recorded history.

The append-only log this replays over is the ``EventLog`` seam (async: one
``replay`` batch-read plus a per-event ``append``), defined in ``history.py``.
``run`` loads the recorded history once and drives over that local cursor, so it
is indifferent to whether the store is in memory or on disk -- which is what
lets SQLite swap in without touching this loop, and lets a durable store fsync
off the event loop rather than blocking it.
"""

import asyncio
import time
from collections.abc import Awaitable, Callable, Coroutine, Mapping
from dataclasses import dataclass, field
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
    SleepCommand,
    TimerFired,
)
from .history import EventLog
from .retry import RetryPolicy, idempotency_key

# An activity is plain side-effectful code: takes JSON args, returns a JSON
# value. It is an ``async def`` so the runtime can ``await`` it -- which is what
# makes a waiting activity cooperative (it parks the workflow without blocking
# the engine) and is the prerequisite for
# ``asyncio.wait_for`` timeouts. It runs once per execution and is never replayed.
#
# Blocking work is NOT auto-wrapped: an activity that must call a synchronous,
# blocking function wraps it itself with ``await asyncio.to_thread(fn, ...)``.
# That explicitness is deliberate -- it is exactly where the "a timeout can only
# abandon the thread, not interrupt it" caveat lives, and we surface it rather
# than hide it behind a silent coercion.
Activity = Callable[..., Awaitable[JsonValue]]


@dataclass(frozen=True)
class ActivitySpec:
    """An activity bound to its execution policies.

    Activities are registered by name alongside the policies that govern how the
    runtime runs them: ``retry`` and ``idempotent`` plus a
    per-attempt ``timeout``. When ``idempotent`` is set the
    runtime injects a stable ``idempotency_key`` keyword arg into each call so
    the activity can dedup across the at-least-once boundary; when ``timeout``
    is set each execution attempt runs under ``asyncio.wait_for``, which cancels
    the activity's task past the budget and raises ``TimeoutError`` -- retriable
    like any failure, and recorded as one ``Failed`` on exhaustion. A bare
    callable may be registered in place of a spec -- it is normalized to a spec
    with the defaults (no retry, not idempotent, no timeout) in :func:`run`
    (see :func:`_normalize_registry`).

    Like a retry policy, a timeout is *execution* machinery, not part of the
    workflow's deterministic history: it lives on the spec, never in the
    ``ActivityCommand`` the workflow yields, so the determinism guard, the
    command schema, and serialization are unchanged, and changing a timeout
    between workflow versions does not trip the guard.
    """

    fn: Activity
    retry: RetryPolicy = field(default_factory=RetryPolicy)
    idempotent: bool = False
    timeout: float | None = None

    def __post_init__(self) -> None:
        # Validate at construction, like RetryPolicy: a malformed spec is a
        # programmer bug and should fail loudly at registration, not mid-run.
        # None means "no wall-clock budget" (the default); 0.0 is allowed --
        # it cancels unless the activity completes synchronously.
        if self.timeout is not None and self.timeout < 0:
            raise ValueError("timeout must be >= 0 or None")


# The registry a caller hands to ``run``: activity name -> either a bare
# callable (default policy) or a full ActivitySpec. Normalized to specs inside
# ``run`` so the rest of the runtime always sees a spec.
ActivityRegistry = Mapping[str, Activity | ActivitySpec]

# The clock a workflow experiences is injected, never read straight from the OS.
# That is what makes timers testable without real wall-clock waiting: a test
# passes a controllable ``now`` and an async ``sleep`` that records instead of
# blocking, and can then assert exact remainder math. Defaults are
# the real OS clock and ``asyncio.sleep`` -- so production behaviour waits
# *cooperatively* (a parked workflow no longer blocks the engine thread), the
# payoff of the async engine. ``now`` stays synchronous: reading the clock is
# instant and never suspends. One clock source -- wall-clock Unix floats, the same
# one ``NowCommand`` reads -- is used everywhere; swap both defaults to monotonic
# in one place if clock-jump robustness is ever needed.
Clock = Callable[[], float]
AsyncSleeper = Callable[[float], Awaitable[None]]


# The seam between the replay loop and *how an activity actually runs*. The loop
# knows an activity only by name + JSON args -- locating it, running it under its
# retry/timeout/idempotency policy, and returning its result (or raising on
# terminal failure) are the executor's concern. run() builds a
# LocalActivityExecutor by default, so the in-process path runs activities
# exactly as before. A remote executor serializes a task and
# awaits a worker process's report -- the SAME loop, a different executor, which
# is the whole point of the seam. execute() returns the result or raises;
# _execute wraps either into one Completed/Failed event, so the determinism model
# is untouched: replay never calls the executor, it feeds the recorded outcome
# back.
class ActivityExecutor(Protocol):
    """Run a named activity and return its result, or raise on terminal failure."""

    async def execute(
        self,
        name: str,
        args: tuple[JsonValue, ...],
        *,
        workflow_id: str | None,
        seq: int,
    ) -> JsonValue: ...


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


class _ActivityExecutionError(Exception):
    """A terminal activity-execution failure, recorded as a ``Failed`` event.

    This is the executor's signal that an activity *ran and failed* (after its
    retry policy was exhausted) -- the one kind of exception that should become a
    recorded outcome. It is deliberately distinct from setup errors: a missing
    activity (``KeyError``) or an idempotent activity run without a workflow_id
    (``ValueError``) are *programmer bugs* that propagate as their own type and
    must NOT be swallowed into a Failed event. Before the
    executor seam, that distinction was expressed by code placement -- setup ran
    before the try, execution inside it; across the executor boundary a marker is
    the honest way to say "the activity failed, the call itself was fine."

    _execute catches exactly this and nothing else, so setup errors pass straight
    through. An activity is free to raise any ``Exception`` at runtime (even a
    ``ValueError``); that becomes an _ActivityExecutionError and thus a Failed,
    which is why the distinction cannot be made by exception type alone.
    """

    def __init__(self, error_type: str, error_message: str) -> None:
        super().__init__(f"{error_type}: {error_message}")
        self.error_type = error_type
        self.error_message = error_message


# --- Internals ---------------------------------------------------------------


async def _execute(
    command: Command,
    executor: ActivityExecutor,
    *,
    now: Clock,
    workflow_id: str | None,
    seq: int,
) -> Event:
    """Run a command for real (first run only) and wrap its outcome in an Event.

    All side effects live here: for an activity, *delegating* to the executor
    (which locates and runs it under its policy -- in-process via the registry,
    or in a worker process); reading the injected clock; or
    stamping a timer's deadline. On success the activity's result is recorded;
    on failure -- after the retry policy is exhausted -- we record a ``Failed``
    event (and the loop re-raises it to abort).

    A ``SleepCommand`` is recorded but NOT waited for here -- it only stamps the
    deadline. The actual wait lives in :func:`_resolve`, which is shared by the
    first-run and replay branches, because a timer resumed mid-sleep must wait
    its remainder on the *replay* path that ``_execute`` never sees.
    """
    match command:
        case ActivityCommand(name, args):
            # The executor owns *how* the activity runs -- lookup, retry,
            # timeout, and idempotency-key injection -- and returns its terminal
            # result or raises after the policy is exhausted. _execute wraps
            # either into a single event, so one invocation is always one
            # Completed/Failed. This branch is the only
            # place the loop reaches outside itself, which is precisely the seam
            # a remote executor fills.
            try:
                result = await executor.execute(name, args, workflow_id=workflow_id, seq=seq)
            except _ActivityExecutionError as exc:
                # The activity ran and failed (after its policy) -- record one
                # Failed. Any other exception is a setup error (missing activity,
                # bad idempotency config) and propagates untouched: fail fast with
                # a clear type, never swallow a programmer bug into a Failed event.
                return Failed(
                    command=command,
                    error_type=exc.error_type,
                    error_message=exc.error_message,
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

    Existing callers register plain functions (``{"greet": greet}``); an
    ``ActivitySpec`` attaches a retry policy. Normalizing once, here, lets
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


def _mint_key(spec: ActivitySpec, name: str, workflow_id: str | None, seq: int) -> str | None:
    """The idempotency key for an invocation, or ``None`` if not idempotent.

    The key is ``"{workflow_id}:{seq}"`` -- stable across the original run, every
    retry, and crash-replay-reexecution, because ``workflow_id`` is fixed and
    deterministic replay lands every command at the same ``seq``
    (:func:`~chronicle.retry.idempotency_key`). It is computed, never stored, so
    it costs nothing in the log. An idempotent activity
    needs a workflow_id to mint a meaningful key; a non-idempotent one never
    mints one, so workflow_id stays optional for the rest of the engine.
    """
    if not spec.idempotent:
        return None
    if workflow_id is None:
        raise ValueError(
            f"activity {name!r} is registered idempotent, so run() "
            f"needs a workflow_id to mint its idempotency key"
        )
    return idempotency_key(workflow_id, seq)


async def _run_activity(
    spec: ActivitySpec,
    args: tuple[JsonValue, ...],
    *,
    key: str | None,
    sleep: AsyncSleeper,
) -> JsonValue:
    """Call ``spec.fn`` under its retry policy, returning the result.

    Retries on any ``Exception`` up to ``spec.retry.max_attempts`` times,
    waiting the policy's backoff between attempts via the injected ``sleep``
    (a plain wait -- NOT a recorded ``SleepCommand`` -- so retries leave no
    trace in the event log). ``BaseException`` is never caught, so
    ``KeyboardInterrupt`` / ``SystemExit`` propagate untouched. When every
    attempt fails, the last exception propagates to :func:`_execute`, which
    records a single ``Failed`` event for the whole sequence. Each attempt also
    runs under the spec's per-attempt ``timeout`` when set: a timed-out attempt
    raises ``TimeoutError``, which is an ordinary ``Exception`` and so is
    retried like any transient failure.

    ``key`` is the idempotency key injected as ``idempotency_key=`` when the
    spec is idempotent (``None`` otherwise). It is built once, before the loop,
    so every retry of the same invocation presents the *same* key -- a retry
    re-runs the same activity, not a new one.

    This runs only on first run / new ground: pure replay never calls it, so
    retries, backoff waits, key injection, and timeouts never happen on replay.
    """
    policy = spec.retry
    # Same key on every attempt: a retry re-runs the SAME invocation, so it must
    # show the downstream system the SAME key.
    kwargs: dict[str, str] = {} if key is None else {"idempotency_key": key}
    attempt = 0
    while True:
        attempt += 1
        try:
            # Per-attempt wall-clock budget. asyncio.wait_for
            # cancels the activity's task past `spec.timeout` seconds and raises
            # TimeoutError -- caught below like any failure, so a timeout is
            # retried per the policy and (on exhaustion) recorded as one Failed.
            # timeout=None skips the wrapper entirely (the default: no budget).
            # NB: a blocking activity that wrapped itself in asyncio.to_thread
            # can only be *abandoned*, not interrupted -- its thread keeps
            # running; we surface that caveat rather than hide it.
            awaitable = spec.fn(*args, **kwargs)
            if spec.timeout is None:
                return await awaitable
            return await asyncio.wait_for(awaitable, spec.timeout)
        except Exception:
            if attempt >= policy.max_attempts:
                raise
            await sleep(policy.backoff_for(attempt))


class LocalActivityExecutor:
    """Run activities in-process against a registry -- the pre-distribution executor.

    This is the executor :func:`run` builds by default from a registry, so every
    call site and test runs activities exactly as before, in the engine
    process. It is also the "local" half of the executor seam: the same ``run``
    loop, handed a *remote* executor instead, dispatches activities to a worker
    process with no other change. Activities are looked up by name, given their
    engine-minted idempotency key, and run under their :class:`ActivitySpec`
    policy via :func:`_run_activity` -- the same loop a worker reuses,
    so retry/timeout/idempotency stays one implementation in one place.
    """

    def __init__(self, registry: Mapping[str, ActivitySpec], *, sleep: AsyncSleeper) -> None:
        self._registry = registry
        self._sleep = sleep

    async def execute(
        self,
        name: str,
        args: tuple[JsonValue, ...],
        *,
        workflow_id: str | None,
        seq: int,
    ) -> JsonValue:
        spec = _require_activity(self._registry, name)  # KeyError: setup, propagates
        key = _mint_key(spec, name, workflow_id, seq)  # ValueError: setup, propagates
        try:
            return await _run_activity(spec, args, key=key, sleep=self._sleep)
        except Exception as exc:  # terminal execution failure -> signal to _execute
            raise _ActivityExecutionError(type(exc).__name__, str(exc)) from exc


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


async def _resolve(event: Event, *, now: Clock, sleep: AsyncSleeper) -> JsonValue:
    """Resolve a recorded event into what the workflow should receive.

    Success -> the recorded result (fed back into the coroutine). Failure ->
    re-raise, so a recorded crash is reproduced on replay instead of silently
    turning into a success. Timer -> wait until the recorded deadline if it is
    still in the future, then return that deadline.

    This is the one place replay can block in real time. A timer resumed
    mid-sleep has a future deadline, so resolving it waits the remainder; pure
    replay of an already-completed workflow never blocks, because every recorded
    deadline is in the past by then. The wait is a side effect only -- the
    *result* fed back is the same deadline either way, so determinism holds.
    """
    match event:
        case Completed():
            return event.result
        case Failed():
            raise ActivityFailedError(event.error_type, event.error_message)
        case TimerFired():
            remaining = event.deadline - now()
            if remaining > 0:
                await sleep(remaining)
            return event.deadline
        case _:
            raise AssertionError(f"unknown event type: {type(event).__name__}")


# --- Public API --------------------------------------------------------------


async def run[R](
    workflow: Callable[..., Coroutine[Any, Any, R]],
    args: tuple[JsonValue, ...],
    log: EventLog,
    registry: ActivityRegistry | None = None,
    *,
    executor: ActivityExecutor | None = None,
    workflow_id: str | None = None,
    now: Clock = time.time,
    sleep: AsyncSleeper = asyncio.sleep,
) -> R:
    """Drive ``workflow`` to completion over ``log``, deterministically.

    This is a *coroutine*: ``await`` it (or wrap a sync call site in
    ``asyncio.run(run(...))``). It creates a fresh workflow coroutine, feeds it
    recorded results for every command it has seen before, and executes + records
    anything new. Returns the workflow's final value. The same call serves first
    run, pure replay, and crash-resume.

    The workflow coroutine itself is still driven by manual ``.send()`` -- that
    does not change. What is async is the *driver*: at each command it ``await``s
    the side-effect's resolution, so a waiting activity or timer *cooperatively*
    parks this run (yielding to the event loop and other workflows) instead of
    blocking the thread. The ``__await__`` bridge in ``context.py`` is
    loop-agnostic, so workflow code is entirely unchanged.

    ``now`` and ``sleep`` are the clock a workflow experiences. ``now`` defaults
    to the real OS clock (synchronous -- reading the clock never suspends);
    ``sleep`` defaults to ``asyncio.sleep`` (cooperative). Tests inject fakes --
    ``now`` a controllable float, ``sleep`` an async function that records
    instead of blocking -- so timer and retry behaviour can be asserted without
    real wall-clock waiting. A durable timer is resolved (possibly waiting its
    remainder) inside this loop via :func:`_resolve`; ``sleep`` is also passed to
    the activity executor for retry backoff (:func:`_run_activity`).

    ``workflow_id`` identifies this execution. It is optional in general but
    required the moment any registered activity is ``idempotent``: the runtime
    mints each such activity a stable key ``"{workflow_id}:{seq}"`` (see
    :func:`~chronicle.retry.idempotency_key`) so it can dedup across the
    at-least-once boundary. ``seq`` is the command's position in the log -- the
    same on every run, by deterministic replay.

    ``registry`` (the default path) runs activities in-process via a
    :class:`LocalActivityExecutor`; pass ``executor=`` instead to run them
    elsewhere -- a remote executor dispatches each activity to a
    worker process. Exactly one of the two is given.
    """
    # The executor is the seam: an explicit ``executor=`` is the
    # distribution path (a remote executor that dispatches to a worker process),
    # while ``registry`` (the default) runs activities in-process via a
    # LocalActivityExecutor -- exactly the same behavior, so every existing
    # call site is unchanged. Bare callables in a registry are normalized to
    # default specs first. Exactly one of the two is given.
    if executor is not None and registry is not None:
        raise ValueError("run(): pass a registry or an executor, not both")
    if executor is None:
        if registry is None:
            raise ValueError("run() requires a registry or an executor")
        executor = LocalActivityExecutor(_normalize_registry(registry), sleep=sleep)
    ctx = WorkflowContext()
    coro = workflow(ctx, *args)
    # Load the recorded history once: an async batch read for a durable store, a
    # copy of the list for the in-memory one. The loop drives over this local
    # cursor -- a command seen before (i < len(history)) is fed its recorded
    # result; new ground is executed, durably appended, and appended here too so
    # the cursor stays in lockstep with the store for the iterations that follow.
    history = await log.replay()
    value_to_send: JsonValue | None = None
    i = 0
    while True:
        try:
            command = coro.send(value_to_send)
        except StopIteration as done:
            # Guard the other direction too: a workflow that returns *fewer*
            # commands than were recorded has diverged from the recorded run.
            if i < len(history):
                raise NonDeterminismError(
                    "non-deterministic workflow: finished after "
                    f"{i} command(s) but {len(history)} were recorded"
                ) from None
            return cast(R, done.value)
        if i < len(history):
            event = history[i]  # REPLAY: seen this command before
            _assert_matches(command, event)
        else:
            event = await _execute(
                command,
                executor,
                now=now,
                workflow_id=workflow_id,
                seq=i,
            )  # NEW GROUND: execute & record
            await log.append(event)  # durably persist (one fsync) before proceeding
            history.append(event)  # advance the cursor to match the store
        value_to_send = await _resolve(event, now=now, sleep=sleep)  # may wait on a timer
        i += 1


__all__ = [
    "Activity",
    "ActivityExecutor",
    "ActivityFailedError",
    "ActivityRegistry",
    "ActivitySpec",
    "AsyncSleeper",
    "Clock",
    "EventLog",
    "LocalActivityExecutor",
    "NonDeterminismError",
    "run",
]
