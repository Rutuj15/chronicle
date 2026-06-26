"""A durable timer survives a worker killed mid-sleep.

Run it with no arguments to see the whole story::

    uv run python examples/durable_timer.py

A workflow reads the clock, sleeps for a few seconds (a *durable* timer), then
reads the clock again. Process 1 records the start time and the timer's deadline,
then begins sleeping -- and we KILL it mid-sleep, before the timer fires.
Process 2 opens the same file cold, replays up to the timer, and waits only the
*remainder* of the original deadline before completing.

That is durable timers made real: the deadline lived on disk (one fsync per
event -- the durability boundary), so the crashed worker's in-flight sleep cost nothing
-- on resume the workflow waited exactly what was left, not the whole duration
again.

Each phase can also be driven directly::

    uv run python examples/durable_timer.py start /tmp/chronicle.db
    uv run python examples/durable_timer.py resume /tmp/chronicle.db
"""

import asyncio
import sqlite3
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from chronicle.context import WorkflowContext
from chronicle.events import JsonValue
from chronicle.history import SqliteEventLog
from chronicle.runtime import run

WORKFLOW_ID = "demo"
DURATION = 6.0  # how long the workflow's timer runs (seconds)
KILL_AFTER = 1.5  # process 1 is killed this far into the sleep (seconds)


async def timed(ctx: WorkflowContext, duration: float) -> dict[str, JsonValue]:
    """Read the clock, sleep (a durable timer), then read the clock again.

    The middle ``await ctx.sleep(duration)`` is what suspends in real time and
    survives the crash: its deadline is recorded *before* the wait, so a worker
    killed mid-sleep loses nothing -- on resume the runtime waits only the
    remainder of that recorded deadline.
    """
    started = await ctx.now()
    deadline = await ctx.sleep(duration)
    finished = await ctx.now()
    return {"started": started, "deadline": deadline, "finished": finished}


def start(db_path: str) -> None:
    """Phase 1: record the start time + timer deadline, then sleep for real.

    Uses the real OS clock and a real ``time.sleep``: a timer must actually
    elapse wall-clock time on first run. The caller kills this process mid-sleep
    to simulate a worker crash; only the recorded events on disk survive it.
    """
    conn = sqlite3.connect(db_path)
    log = SqliteEventLog(conn, WORKFLOW_ID)
    print(f"  timer duration = {DURATION:.1f}s")
    print("  recording start time + deadline, then sleeping for real...")
    sys.stdout.flush()  # narration must land before the blocking sleep below
    # The default sleep is asyncio.sleep, so the worker truly waits the duration
    # (cooperatively) before the caller kills it mid-sleep.
    asyncio.run(run(timed, (DURATION,), log, {}))
    conn.close()


def resume(db_path: str) -> None:
    """Phase 2: reopen the file cold and resume -- wait only the remainder.

    The real OS clock (``run``'s default ``now``) is essential here: the
    remainder is ``recorded_deadline - now()``, and the deadline was stamped with
    the same wall clock in process 1.
    """
    reopened_at = time.time()

    async def announce_sleep(remaining: float) -> None:
        print(f"  timer not due yet -- {remaining:.2f}s of the {DURATION:.1f}s left; waiting...")
        await asyncio.sleep(remaining)

    conn = sqlite3.connect(db_path)
    log = SqliteEventLog(conn, WORKFLOW_ID)
    result = asyncio.run(run(timed, (DURATION,), log, {}, sleep=announce_sleep))
    conn.close()

    out: dict[str, JsonValue] = result
    started = float(out["started"])
    deadline = float(out["deadline"])
    finished = float(out["finished"])
    print(f"  reopened at   t={reopened_at:.3f}")
    print(f"  started       t={started:.3f}   (recorded by process 1)")
    print(f"  deadline      t={deadline:.3f}   (= started + {DURATION:.1f}s)")
    print(f"  finished      t={finished:.3f}   (recorded by process 2)")
    print(f"  span          {finished - started:.3f}s across two processes")


def _kill_after(process: subprocess.Popen[bytes], delay: float) -> int:
    """Run ``process`` and SIGKILL it after ``delay`` seconds; return its exit code."""
    time.sleep(delay)
    process.kill()  # SIGKILL: the in-flight time.sleep dies with the process
    return process.wait()


def main() -> int:
    args = sys.argv[1:]
    if len(args) == 2 and args[0] == "start":
        start(args[1])
        return 0
    if len(args) == 2 and args[0] == "resume":
        resume(args[1])
        return 0

    # No subcommand: run the full two-process story in a throwaway database.
    with tempfile.TemporaryDirectory() as tmp:
        db_path = str(Path(tmp) / "chronicle.db")
        script = str(Path(__file__).resolve())

        print("Phase 1 -- start (process 1)")
        print("  records the start time + timer deadline, then sleeps for real.")
        sys.stdout.flush()  # land the header before the child's output
        proc = subprocess.Popen([sys.executable, script, "start", db_path])
        code = _kill_after(proc, KILL_AFTER)
        print(f"\n  *** process 1 killed mid-sleep (exit {code}) -- only the recorded")
        print("      deadline on disk survives; the in-flight sleep is lost ***\n")

        print("Phase 2 -- resume (process 2, cold open)")
        print("  reopens the same file, replays to the timer, waits the remainder.")
        sys.stdout.flush()
        subprocess.run([sys.executable, script, "resume", db_path], check=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
