"""Shared test helpers for the Chronicle suite.

The engine's ``run`` is a coroutine (Week 5: the driver is async). These helpers
let the tests stay synchronous ``def test_...`` -- with no pytest-asyncio
dependency -- while exercising the full async path:

* :func:`run_sync` -- :func:`asyncio.run` around :func:`run`; tests call it where
  they used to call ``run`` directly.
* :class:`FakeClock` -- a controllable clock: a mutable ``now`` plus an *async*
  ``sleep`` that advances time and records each wait without blocking. Replaces
  the per-file fakes the async conversion made worth centralizing.
* :func:`noop_sleep` -- an async sleep that does nothing, for retry/idempotency
  tests that only care about attempt counts.

Pytest's prepend import mode puts this directory on ``sys.path``, so test modules
import these as ``from conftest import run_sync, FakeClock, noop_sleep``.
"""

import asyncio
import time
from collections.abc import Callable, Coroutine
from typing import Any

from chronicle.events import JsonValue
from chronicle.runtime import ActivityRegistry, AsyncSleeper, Clock, EventLog, run


def run_sync[R](
    workflow: Callable[..., Coroutine[Any, Any, R]],
    args: tuple[JsonValue, ...],
    log: EventLog,
    registry: ActivityRegistry,
    *,
    workflow_id: str | None = None,
    now: Clock = time.time,
    sleep: AsyncSleeper = asyncio.sleep,
) -> R:
    """Run the async engine to completion under a fresh event loop.

    A synchronous adapter around the coroutine :func:`run`: it mirrors ``run``'s
    signature and drives it via :func:`asyncio.run`, so the suite stays
    dependency-free (no pytest-asyncio) while exercising the real async path.
    """
    return asyncio.run(
        run(workflow, args, log, registry, workflow_id=workflow_id, now=now, sleep=sleep)
    )


class FakeClock:
    """A controllable stand-in for the OS clock and async sleep.

    ``now`` returns a mutable float; ``sleep`` (async) advances it and records
    each wait, but never blocks the event loop -- so timer and retry tests can
    assert exact durations and backoff schedules with no real wall-clock waiting.
    """

    def __init__(self, start: float = 0.0) -> None:
        self.time = start
        self.waits: list[float] = []

    def now(self) -> float:
        return self.time

    async def sleep(self, duration: float) -> None:
        self.time += duration
        self.waits.append(duration)


async def noop_sleep(_: float) -> None:
    """An async sleep that does nothing.

    Retry and idempotency tests only care about attempt counts, not elapsed
    backoff -- this satisfies the async sleep seam without advancing a clock or
    blocking.
    """
