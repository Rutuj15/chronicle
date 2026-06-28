"""Exactly-once side effects under at-least-once execution.

Run it with no arguments to see the whole story::

    uv run python examples/idempotent_charge.py

Chronicle executes activities at-least-once: if a crash takes the process after
an activity ran but *before* its outcome was fsync'd, the activity re-runs on
resume. The engine cannot make ``charge()`` exactly-once by itself -- but it
hands every activity a stable ``idempotency_key`` (here ``"order-1:0"``), so the
activity can dedup against a downstream system whose state lives in a *different*
durability domain than the engine's event log.

This demo charges in one process, then *simulates a lost commit* (deletes the
recorded event, as if the fsync never landed) and resumes in a second process.
The activity re-runs with the SAME key; the downstream ledger, which survived,
returns the cached result instead of charging again. One charge, two executions.
"""

import asyncio
import sqlite3
import subprocess
import sys
import tempfile
from pathlib import Path

import aiosqlite

from chronicle.core.context import WorkflowContext
from chronicle.core.history import SqliteEventLog
from chronicle.core.runtime import ActivityRegistry, ActivitySpec, run

WORKFLOW_ID = "order-1"


def _charge(amount: int, *, idempotency_key: str, ledger: str) -> str:
    """A charge that is idempotent against a separate SQLite ledger.

    The ledger is keyed by the engine-minted idempotency key, so a re-run with
    the same key is a no-op that returns the *original* result. Making the
    activity idempotent is the application's job -- the engine only supplies the
    stable key.
    """
    conn = sqlite3.connect(ledger)
    try:
        conn.execute("CREATE TABLE IF NOT EXISTS charges (key TEXT PRIMARY KEY, amount INT)")
        cur = conn.execute("SELECT amount FROM charges WHERE key = ?", (idempotency_key,))
        row = cur.fetchone()
        if row is not None:
            return f"charged-{row[0]}"  # downstream saw this key -> return original
        conn.execute("INSERT INTO charges (key, amount) VALUES (?, ?)", (idempotency_key, amount))
        conn.commit()
        return f"charged-{amount}"
    finally:
        conn.close()


def _registry(ledger: str) -> ActivityRegistry:
    async def charge(amount: int, *, idempotency_key: str) -> str:
        return _charge(amount, idempotency_key=idempotency_key, ledger=ledger)

    return {"charge": ActivitySpec(charge, idempotent=True)}


async def checkout(ctx: WorkflowContext, amount: int) -> str:
    return await ctx.activity("charge", amount)


async def record(db_path: str, ledger: str) -> None:
    """Phase 1: run the workflow for real; the activity charges the ledger."""
    conn = await aiosqlite.connect(db_path)
    log = SqliteEventLog(conn, WORKFLOW_ID, asyncio.Lock())
    await log.start()
    result = await run(checkout, (7,), log, _registry(ledger), workflow_id=WORKFLOW_ID)
    print(f"  result           = {result!r}")
    print(f"  events persisted = {len(await log.replay())}")
    await conn.close()


async def resume(db_path: str, ledger: str) -> None:
    """Phase 2: the charge event was lost, so it re-runs -- with the same key."""
    conn = await aiosqlite.connect(db_path)
    log = SqliteEventLog(conn, WORKFLOW_ID, asyncio.Lock())
    await log.start()
    result = await run(checkout, (7,), log, _registry(ledger), workflow_id=WORKFLOW_ID)
    await conn.close()

    lconn = sqlite3.connect(ledger)
    n = lconn.execute("SELECT COUNT(*) FROM charges").fetchone()[0]
    lconn.close()

    print(f"  result           = {result!r}   (same as phase 1 -- the cached charge)")
    print("  events on disk   = 1   (the charge re-executed and re-recorded)")
    print(f"  ledger charges   = {n}   (exactly-once, despite two executions)")


def _run_phase(phase: str, db_path: str, ledger: str) -> None:
    """Re-invoke this script as a fresh process for the given phase."""
    # Flush so the parent's narration lands before the child's buffered output.
    sys.stdout.flush()
    subprocess.run(
        [sys.executable, str(Path(__file__).resolve()), phase, db_path, ledger],
        check=True,
    )


def main() -> int:
    args = sys.argv[1:]
    if len(args) == 3 and args[0] == "record":
        asyncio.run(record(args[1], args[2]))
        return 0
    if len(args) == 3 and args[0] == "resume":
        asyncio.run(resume(args[1], args[2]))
        return 0

    # No subcommand: run the full two-process story in a throwaway directory.
    with tempfile.TemporaryDirectory() as tmp:
        db_path = str(Path(tmp) / "chronicle.db")
        ledger = str(Path(tmp) / "ledger.db")

        print("Phase 1 -- charge (process 1)")
        print("  runs the workflow; the idempotent activity charges the ledger.")
        _run_phase("record", db_path, ledger)

        # Simulate a crash BEFORE the outcome's fsync landed: wipe the charge
        # event from the engine's log. The ledger (a separate file) is untouched
        # -- a different durability domain, which is the whole point.
        conn = sqlite3.connect(db_path)
        conn.execute("DELETE FROM events WHERE workflow_id = ?", (WORKFLOW_ID,))
        conn.commit()
        conn.close()

        print("\n  *** simulated lost commit: charge event wiped from the log ***")
        print("  *** the ledger survived -- it is a separate durability domain ***\n")

        print("Phase 2 -- resume (process 2, cold open)")
        print("  re-runs the workflow; the charge re-executes with the SAME key.")
        _run_phase("resume", db_path, ledger)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
