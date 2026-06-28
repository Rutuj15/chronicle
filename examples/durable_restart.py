"""Durable execution across a real process death.

Run it with no arguments to see the whole story::

    uv run python examples/durable_restart.py

Chronicle records a workflow's history to SQLite in one process; that process
exits; then a *second* process opens the same file cold and replays the workflow
to the identical result -- WITHOUT re-running a single activity. That gap
between the two processes is durable execution made real.

Each event is its own committed transaction under ``synchronous = FULL`` (an
fsync), so the history is durable the moment it is recorded -- survival comes
from the per-event commit, not from a graceful shutdown. Each phase can also be
driven directly::

    uv run python examples/durable_restart.py record /tmp/chronicle.db
    uv run python examples/durable_restart.py replay /tmp/chronicle.db
"""

import asyncio
import subprocess
import sys
import tempfile
from pathlib import Path

import aiosqlite

from chronicle.context import WorkflowContext
from chronicle.history import SqliteEventLog
from chronicle.runtime import ActivityRegistry, run

WORKFLOW_ID = "demo"


def _registry(executions: dict[str, int]) -> ActivityRegistry:
    """Activities that count how many times they *actually run* in this process.

    On first run the counters climb; on replay they stay at zero -- the proof
    that replayed activities are fed back from history, not re-executed.
    """

    async def greet(name: str) -> str:
        executions["greet"] += 1
        return f"hello {name}"

    async def shout(text: str) -> str:
        executions["shout"] += 1
        return text.upper()

    return {"greet": greet, "shout": shout}


async def two_step(ctx: WorkflowContext, name: str) -> str:
    """Greet, then shout the greeting -- two activities, recorded in order."""
    greeting = await ctx.activity("greet", name)
    shouted = await ctx.activity("shout", greeting)
    return f"{greeting} >>> {shouted}"


async def record(db_path: str) -> None:
    """Phase 1: run the workflow for real, persisting every event to disk."""
    executions: dict[str, int] = {"greet": 0, "shout": 0}
    conn = await aiosqlite.connect(db_path)
    log = SqliteEventLog(conn, WORKFLOW_ID, asyncio.Lock())
    await log.start()
    result = await run(two_step, ("world",), log, _registry(executions))
    n_events = len(await log.replay())
    await conn.close()

    print(f"  result           = {result!r}")
    print(f"  activities ran   = {executions}   (both executed)")
    print(f"  events persisted = {n_events}")


async def replay(db_path: str) -> None:
    """Phase 2: cold-open the same file and replay -- no activity re-runs."""
    executions: dict[str, int] = {"greet": 0, "shout": 0}
    conn = await aiosqlite.connect(db_path)
    log = SqliteEventLog(conn, WORKFLOW_ID, asyncio.Lock())
    await log.start()
    result = await run(two_step, ("world",), log, _registry(executions))
    n_events = len(await log.replay())
    await conn.close()

    print(f"  result           = {result!r}")
    print(f"  activities ran   = {executions}   (NONE re-ran)")
    print(f"  events on disk   = {n_events}")


def _run_phase(phase: str, db_path: str) -> None:
    """Re-invoke this script as a fresh process for the given phase."""
    # Flush so this process's narration lands before the child's output (stdout
    # is block-buffered when piped, so the child's exit-flush would otherwise
    # overtake the parent's buffered prints and scramble the order).
    sys.stdout.flush()
    subprocess.run(
        [sys.executable, str(Path(__file__).resolve()), phase, db_path],
        check=True,
    )


def main() -> int:
    args = sys.argv[1:]
    if len(args) == 2 and args[0] == "record":
        asyncio.run(record(args[1]))
        return 0
    if len(args) == 2 and args[0] == "replay":
        asyncio.run(replay(args[1]))
        return 0

    # No subcommand: run the full two-process story in a throwaway database.
    with tempfile.TemporaryDirectory() as tmp:
        db_path = str(Path(tmp) / "chronicle.db")

        print("Phase 1 -- record (process 1)")
        print("  runs the workflow, persists the event log to SQLite, then exits.")
        _run_phase("record", db_path)

        print("\n  *** process 1 has exited -- nothing it held in memory survives ***\n")

        print("Phase 2 -- replay (process 2, cold open)")
        print("  opens the same file and re-runs the workflow from history.")
        _run_phase("replay", db_path)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
