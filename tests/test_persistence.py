"""Durability tests: the event log survives a real process restart.

The same replay loop now runs over a SQLite-backed log. These tests
prove the log genuinely lives on disk -- not in memory -- by discarding the
runtime, the log object, and even the database connection between recording and
replaying, and showing zero activity re-execution and an identical result.
One commit (an fsync) per event is the durability boundary.
"""

import sqlite3
from pathlib import Path

from chronicle.context import WorkflowContext
from chronicle.events import ActivityCommand, Completed, JsonValue
from chronicle.history import SqliteEventLog
from chronicle.runtime import ActivityRegistry
from conftest import FakeClock, noop_sleep, run_sync


def _counting_registry() -> tuple[ActivityRegistry, dict[str, int]]:
    calls: dict[str, int] = {"greet": 0, "shout": 0}

    async def greet(name: str) -> str:
        calls["greet"] += 1
        return f"hello {name}"

    async def shout(text: str) -> str:
        calls["shout"] += 1
        return text.upper()

    return {"greet": greet, "shout": shout}, calls


async def two_step(ctx: WorkflowContext, name: str) -> str:
    greeting = await ctx.activity("greet", name)
    shouted = await ctx.activity("shout", greeting)
    return f"{greeting} >>> {shouted}"


async def sleep_once(ctx: WorkflowContext, duration: float) -> JsonValue:
    """Sleep once; returns the recorded deadline (a durable timer)."""
    return await ctx.sleep(duration)


# --- SQLite behaves like the in-memory log under the replay loop --------------


def test_sqlite_log_records_and_replays(tmp_path: Path) -> None:
    registry, calls = _counting_registry()
    conn = sqlite3.connect(str(tmp_path / "chronicle.db"))
    log = SqliteEventLog(conn, "wf")

    result = run_sync(two_step, ("world",), log, registry)

    assert result == "hello world >>> HELLO WORLD"
    assert calls == {"greet": 1, "shout": 1}
    assert len(log) == 2
    first = log[0]
    assert isinstance(first, Completed)
    assert isinstance(first.command, ActivityCommand)
    assert first.command.name == "greet"

    # Pure replay over the on-disk log: nothing re-runs, nothing new is appended.
    calls["greet"] = calls["shout"] = 0
    replayed = run_sync(two_step, ("world",), log, registry)

    assert replayed == result
    assert calls == {"greet": 0, "shout": 0}
    assert len(log) == 2
    conn.close()


# --- the headline: the log survives discarding connection + runtime ----------


def test_log_survives_connection_reopen(tmp_path: Path) -> None:
    """Discard the runtime, the log, AND the connection -- the log is on disk.

    This is the in-process stand-in for a process death: nothing the first run
    held in memory exists when the second connection opens the same file cold.
    """
    registry, _calls = _counting_registry()
    path = str(tmp_path / "chronicle.db")

    conn1 = sqlite3.connect(path)
    log1 = SqliteEventLog(conn1, "wf")
    run_sync(two_step, ("world",), log1, registry)
    conn1.close()  # the process "dies": no in-memory state survives

    # A brand-new process opens the same file cold and rebuilds the log.
    registry, calls = _counting_registry()  # fresh activities, zero executions
    conn2 = sqlite3.connect(path)
    log2 = SqliteEventLog(conn2, "wf")

    result = run_sync(two_step, ("world",), log2, registry)

    assert result == "hello world >>> HELLO WORLD"
    assert len(log2) == 2  # the history was read back from disk
    assert calls == {"greet": 0, "shout": 0}  # nothing re-ran
    conn2.close()


def test_resume_from_partial_persisted_prefix(tmp_path: Path) -> None:
    """A crash after only the first event was persisted: reopen and resume.

    Mirrors the in-memory prefix test, but the prefix is real rows on disk and the
    resume replays them from SQLite rather than from a Python list.
    """
    registry, _calls = _counting_registry()
    conn = sqlite3.connect(str(tmp_path / "chronicle.db"))

    full = SqliteEventLog(conn, "full")
    run_sync(two_step, ("world",), full, registry)  # records seq 0 and 1

    # Simulate a crash after only seq 0 reached disk: a fresh, empty log holding
    # just the first recorded event.
    partial = SqliteEventLog(conn, "partial")
    partial.append(full[0])

    registry, calls = _counting_registry()
    resumed = run_sync(two_step, ("world",), partial, registry)

    assert resumed == "hello world >>> HELLO WORLD"
    assert calls == {"greet": 0, "shout": 1}  # greet replayed, shout executed
    assert len(partial) == 2  # the missing event was recorded on resume
    conn.close()


# --- multi-tenancy and the durability contract -------------------------------


def test_workflow_ids_are_isolated_in_one_file(tmp_path: Path) -> None:
    conn = sqlite3.connect(str(tmp_path / "chronicle.db"))
    log_a = SqliteEventLog(conn, "workflow-a")
    log_b = SqliteEventLog(conn, "workflow-b")

    registry, _calls = _counting_registry()
    run_sync(two_step, ("world",), log_a, registry)  # 2 events under workflow-a

    assert len(log_a) == 2
    assert len(log_b) == 0  # workflow-b sees none of workflow-a's history
    conn.close()


def test_enforces_full_synchronous_for_durable_commits(tmp_path: Path) -> None:
    conn = sqlite3.connect(str(tmp_path / "chronicle.db"))
    SqliteEventLog(conn, "wf")

    # FULL (== 2) means every commit fsyncs before returning -- the contract
    # that makes one-commit-per-event a real durability boundary.
    level = conn.execute("PRAGMA synchronous").fetchone()
    conn.close()

    assert level is not None
    assert level[0] == 2  # 0=OFF, 1=NORMAL, 2=FULL, 3=EXTRA


# --- a durable timer survives a cold reopen and waits only the remainder ------


def test_timer_survives_reopen_and_waits_remainder(tmp_path: Path) -> None:
    """A durable timer's deadline persists: reopen the file mid-sleep and resume.

    Process 1 records a 10s timer (deadline stamped from now=1000) and exits;
    process 2 opens the same file cold at now=1005 and waits only the 5s
    remainder. The deadline lived on disk, not in either process's memory.
    """
    path = str(tmp_path / "chronicle.db")
    conn1 = sqlite3.connect(path)
    log1 = SqliteEventLog(conn1, "wf")
    run_sync(
        sleep_once,
        (10.0,),
        log1,
        {},
        now=lambda: 1000.0,
        sleep=noop_sleep,  # record without waiting
    )
    conn1.close()  # process 1 "dies" mid-sleep

    # Process 2 opens the same file cold, 5s before the recorded deadline.
    conn2 = sqlite3.connect(path)
    log2 = SqliteEventLog(conn2, "wf")
    clock = FakeClock()
    result = run_sync(
        sleep_once,
        (10.0,),
        log2,
        {},
        now=lambda: 1005.0,
        sleep=clock.sleep,
    )
    assert result == 1010.0
    assert clock.waits == [5.0]  # the persisted deadline, respected after a cold reopen
    assert len(log2) == 1  # nothing re-recorded on resume
    conn2.close()
