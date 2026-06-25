"""Week 5, slice 1 payoff: cooperative concurrency on the async engine.

Run it::

    uv run python examples/concurrent_workflows.py

Before Week 5 the engine ran synchronously to completion: one ``run`` drove one
workflow and executed every activity inline, blocking the thread. A waiting
activity (or a timer) therefore stalled *everything* -- a second workflow could
not progress until the first's wait finished.

Now ``run`` is a coroutine and activities are ``async def``. A workflow that
``await``s an activity *cooperatively* parks itself, yielding to the event loop
so other workflows can advance. Two workflows, each running a 2-second activity,
complete together in ~2 seconds -- not ~4. That overlap is the payoff of
step-and-suspend, and the foundation the distributed workers (Week 5, slice 3)
build on: an activity that blocks on network IO will no longer freeze the engine.
"""

import asyncio
import time

from chronicle.context import WorkflowContext
from chronicle.events import JsonValue
from chronicle.history import InMemoryEventLog
from chronicle.runtime import ActivityRegistry, run

SLOW_FOR = 2.0  # seconds each workflow's activity spends "doing work"


async def nap(label: str) -> str:
    """A slow activity: sleeps for real, standing in for network or DB IO.

    The ``await asyncio.sleep`` is the cooperative suspension point -- it parks
    only *this* activity, letting the event loop run a sibling workflow.
    """
    await asyncio.sleep(SLOW_FOR)
    return f"{label} done"


async def slow(ctx: WorkflowContext, label: str) -> dict[str, JsonValue]:
    """Run one slow activity and report the wall-clock span it observed.

    Both ``ctx.now()`` reads and the activity go through the engine, so this is a
    real (if tiny) workflow -- not a bare coroutine -- exercising the full async
    driver.
    """
    started = await ctx.now()
    await ctx.activity("nap", label)
    finished = await ctx.now()
    return {"label": label, "span": round(finished - started, 2)}


async def both() -> tuple[float, dict[str, JsonValue], dict[str, JsonValue]]:
    """Drive two workflows concurrently; return total elapsed and both spans."""
    registry: ActivityRegistry = {"nap": nap}
    started = time.perf_counter()
    a, b = await asyncio.gather(
        run(slow, ("A",), InMemoryEventLog(), registry),
        run(slow, ("B",), InMemoryEventLog(), registry),
    )
    return time.perf_counter() - started, a, b


def main() -> int:
    elapsed, a, b = asyncio.run(both())
    print(f"  workflow A: {a}")
    print(f"  workflow B: {b}")
    print(f"  each activity takes {SLOW_FOR:.1f}s; both ran concurrently")
    print(f"  total wall-clock = {elapsed:.2f}s   (~{SLOW_FOR:.1f}s, not ~{2 * SLOW_FOR:.1f}s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
