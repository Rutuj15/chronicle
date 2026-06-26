"""Retry policy for activities.

Activities execute at-least-once (one fsync per event is the durability boundary) and may
fail transiently. A :class:`RetryPolicy` says how many times to try an activity
before its failure is recorded, and how long to wait between attempts.

The policy is *execution-time* machinery, not part of the workflow's
deterministic history. It lives next to the activity at registration
(:class:`~chronicle.runtime.ActivitySpec`) rather than inside the
``ActivityCommand`` the workflow yields. That separation is deliberate: the
event log records *what happened* (a ``Completed`` or ``Failed`` outcome); the
policy governs *how hard the runtime tried* to get there. On replay the outcome
is fed back from history and the policy is never consulted, so changing a
policy between workflow versions does not trip the determinism guard -- the
same separation Temporal makes (retry/timeout are not part of the determinism
key).

Backoff waits reuse the injected ``sleep`` the durable-timer path uses, but
directly: a backoff is an ordinary wait, NOT a recorded ``SleepCommand``, so
nothing about a retry enters the event log. That also makes the backoff
schedule assertable in tests without any real wall-clock waiting.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class RetryPolicy:
    """How an activity is retried before its failure is recorded.

    Attributes:
        max_attempts: total attempts including the first. ``1`` means try once
            and never retry -- the default. Must be ``>= 1``.
        initial_backoff: seconds to wait before the *second* attempt. ``0.0``
            retries immediately. Must be ``>= 0``.
        backoff_factor: multiply the last wait by this after each failure,
            giving exponential backoff ``initial * factor**(attempt-1)``.
        max_backoff: cap on any single inter-attempt wait, so exponential
            growth cannot run away. Must be ``>= 0``.
    """

    max_attempts: int = 1
    initial_backoff: float = 0.0
    backoff_factor: float = 2.0
    max_backoff: float = float("inf")

    def __post_init__(self) -> None:
        # Validate at construction -- a malformed policy is a programmer bug and
        # should fail loudly at registration, not mid-retry on first failure.
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if self.initial_backoff < 0:
            raise ValueError("initial_backoff must be >= 0")
        if self.backoff_factor < 0:
            raise ValueError("backoff_factor must be >= 0")
        if self.max_backoff < 0:
            raise ValueError("max_backoff must be >= 0")

    def backoff_for(self, attempt: int) -> float:
        """Seconds to wait *before* retrying, given ``attempt`` just failed.

        ``attempt`` is the 1-based number of the attempt that just failed, so
        ``backoff_for(1)`` is the wait before the second attempt. Growth is
        exponential (``initial * factor**(attempt-1)``), capped at
        ``max_backoff``.
        """
        if attempt < 1:
            raise ValueError("attempt must be >= 1")
        raw = self.initial_backoff * (self.backoff_factor ** (attempt - 1))
        return min(raw, self.max_backoff)


def idempotency_key(workflow_id: str, seq: int) -> str:
    """The stable per-invocation key an idempotent activity receives.

    Activities are at-least-once: a crash after execution but before the outcome
    is fsync'd re-runs the activity on resume, and a retry re-runs it within a
    run. The engine cannot make a side-effecting activity exactly-once -- but it
    *can* hand it a key identical on the original run, on every retry, and on
    crash-replay-reexecution, so the activity (or the downstream system) can
    dedup its own effects.

    The key derives from the workflow run and the command's position in the event
    log. Both are stable by construction: ``workflow_id`` is fixed for the run,
    and deterministic replay guarantees the same command lands at the same
    position every time. So the same invocation always receives the same key --
    across retries and across the crash boundary.

    Note the key is *computed*, never stored: it is not part of the command or
    the event, so it costs nothing in the log. The dedup *state* lives wherever
    the activity puts it (an external system, a table) -- a separate durability
    domain from the engine's own event log, which is exactly why it survives a
    lost commit.
    """
    return f"{workflow_id}:{seq}"


__all__ = ["RetryPolicy", "idempotency_key"]
