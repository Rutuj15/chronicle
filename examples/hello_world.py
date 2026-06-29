"""The smallest Chronicle workflow: one activity, durable across runs.

Run it twice against the same database::

    uv run python examples/hello_world.py
    uv run python examples/hello_world.py

The first run executes the activity and records the result. The second run
replays the recorded history and returns the SAME result WITHOUT re-running the
activity -- durable execution in one file. The gap between the two runs stands
in for a crash: nothing the first process held in memory survived, yet the
workflow picks up exactly where it left off.

The history lives in ``hello_world.db`` next to this script (gitignored). Delete
it to start over and see the activity run again.
"""

import asyncio
from pathlib import Path

import aiosqlite

from chronicle.core.context import WorkflowContext
from chronicle.core.history import SqliteEventLog
from chronicle.core.runtime import ActivityRegistry, run

DB = Path(__file__).resolve().parent / "hello_world.db"
WORKFLOW_ID = "hello"


async def greet(name: str) -> str:
    """An activity: real side effects (HTTP, DB, files) live here.

    It runs once per execution and is never replayed, so this print appears only
    when the activity *actually* runs -- which is exactly how you can see replay
    at work: on the second run it stays silent.
    """
    print(f"    [activity] greet ran for {name!r}")
    return f"Hello {name}"


async def hello(ctx: WorkflowContext, name: str) -> str:
    """A workflow: deterministic orchestration.

    No IO, no randomness, no wall-clock -- every side effect goes through
    ``ctx.activity(...)``. The runtime drives this coroutine and intercepts each
    activity call as a command.
    """
    return await ctx.activity("greet", name)


async def main() -> None:
    registry: ActivityRegistry = {"greet": greet}
    conn = await aiosqlite.connect(DB)
    log = SqliteEventLog(conn, WORKFLOW_ID, asyncio.Lock())
    await log.start()
    try:
        already = len(await log.replay())
        if already:
            print(f"  {already} event(s) on disk -> replay (the activity will NOT re-execute)")
        else:
            print("  0 events on disk -> first run (the activity will execute)")
        result = await run(hello, ("world",), log, registry)
        print(f"  result = {result!r}")
    finally:
        await conn.close()


if __name__ == "__main__":
    print("hello_world:")
    asyncio.run(main())
