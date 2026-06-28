"""Durability tests: the event log survives a real process restart.

The same replay loop now runs over an aiosqlite-backed log. These tests prove
the log genuinely lives on disk -- not in memory -- by discarding the runtime,
the log object, and even the database connection between recording and
replaying, and showing zero activity re-execution and an identical result.
One commit (an fsync) per event is the durability boundary.

The tests are ``async def`` (pytest-asyncio auto mode) and call ``run`` directly
rather than via ``run_sync``: an ``aiosqlite`` connection is bound to the event
loop it was opened on, so the connection, the log, and ``run`` must all share
one loop. A fresh ``asyncio.Lock`` per connection serializes each log's
multi-call sequences on it (the task-queue lesson).
"""

import asyncio
from pathlib import Path

import aiosqlite

from chronicle.core.context import WorkflowContext
from chronicle.core.events import ActivityCommand, Completed, JsonValue
from chronicle.core.history import SqliteEventLog
from chronicle.core.runtime import ActivityRegistry, run
from conftest import FakeClock, noop_sleep


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


async def _open_log(
    conn: aiosqlite.Connection, workflow_id: str, lock: asyncio.Lock
) -> SqliteEventLog:
    """A ready-to-use durable log: constructed and schema ensured."""
    log = SqliteEventLog(conn, workflow_id, lock)
    await log.start()
    return log


# --- SQLite behaves like the in-memory log under the replay loop --------------


async def test_sqlite_log_records_and_replays(tmp_path: Path) -> None:
    registry, calls = _counting_registry()
    lock = asyncio.Lock()
    conn = await aiosqlite.connect(str(tmp_path / "chronicle.db"))
    log = await _open_log(conn, "wf", lock)

    result = await run(two_step, ("world",), log, registry)

    assert result == "hello world >>> HELLO WORLD"
    assert calls == {"greet": 1, "shout": 1}
    history = await log.replay()
    assert len(history) == 2
    first = history[0]
    assert isinstance(first, Completed)
    assert isinstance(first.command, ActivityCommand)
    assert first.command.name == "greet"

    # Pure replay over the on-disk log: nothing re-runs, nothing new is appended.
    calls["greet"] = calls["shout"] = 0
    replayed = await run(two_step, ("world",), log, registry)

    assert replayed == result
    assert calls == {"greet": 0, "shout": 0}
    assert len(await log.replay()) == 2
    await conn.close()


# --- the headline: the log survives discarding connection + runtime ----------


async def test_log_survives_connection_reopen(tmp_path: Path) -> None:
    """Discard the runtime, the log, AND the connection -- the log is on disk.

    This is the in-process stand-in for a process death: nothing the first run
    held in memory exists when the second connection opens the same file cold.
    """
    registry, _calls = _counting_registry()
    path = str(tmp_path / "chronicle.db")

    conn1 = await aiosqlite.connect(path)
    log1 = await _open_log(conn1, "wf", asyncio.Lock())
    await run(two_step, ("world",), log1, registry)
    await conn1.close()  # the process "dies": no in-memory state survives

    # A brand-new process opens the same file cold and rebuilds the log.
    registry, calls = _counting_registry()  # fresh activities, zero executions
    conn2 = await aiosqlite.connect(path)
    log2 = await _open_log(conn2, "wf", asyncio.Lock())

    result = await run(two_step, ("world",), log2, registry)

    assert result == "hello world >>> HELLO WORLD"
    assert len(await log2.replay()) == 2  # the history was read back from disk
    assert calls == {"greet": 0, "shout": 0}  # nothing re-ran
    await conn2.close()


async def test_resume_from_partial_persisted_prefix(tmp_path: Path) -> None:
    """A crash after only the first event was persisted: reopen and resume.

    Mirrors the in-memory prefix test, but the prefix is real rows on disk and
    the resume replays them from SQLite rather than from a Python list.
    """
    registry, _calls = _counting_registry()
    lock = asyncio.Lock()
    conn = await aiosqlite.connect(str(tmp_path / "chronicle.db"))

    full = await _open_log(conn, "full", lock)
    await run(two_step, ("world",), full, registry)  # records seq 0 and 1
    full_history = await full.replay()

    # Simulate a crash after only seq 0 reached disk: a fresh log for a new
    # workflow id holding just the first recorded event.
    partial = await _open_log(conn, "partial", lock)
    await partial.append(full_history[0])

    registry, calls = _counting_registry()
    resumed = await run(two_step, ("world",), partial, registry)

    assert resumed == "hello world >>> HELLO WORLD"
    assert calls == {"greet": 0, "shout": 1}  # greet replayed, shout executed
    assert len(await partial.replay()) == 2  # the missing event was recorded on resume
    await conn.close()


# --- multi-tenancy and the durability contract -------------------------------


async def test_workflow_ids_are_isolated_in_one_file(tmp_path: Path) -> None:
    lock = asyncio.Lock()  # one lock for both logs: they share the connection
    conn = await aiosqlite.connect(str(tmp_path / "chronicle.db"))
    log_a = await _open_log(conn, "workflow-a", lock)
    log_b = await _open_log(conn, "workflow-b", lock)

    registry, _calls = _counting_registry()
    await run(two_step, ("world",), log_a, registry)  # 2 events under workflow-a

    assert len(await log_a.replay()) == 2
    assert len(await log_b.replay()) == 0  # workflow-b sees none of workflow-a's history
    await conn.close()


async def test_enforces_full_synchronous_for_durable_commits(tmp_path: Path) -> None:
    conn = await aiosqlite.connect(str(tmp_path / "chronicle.db"))
    await _open_log(conn, "wf", asyncio.Lock())

    # FULL (== 2) means every commit fsyncs before returning -- the contract
    # that makes one-commit-per-event a real durability boundary.
    cur = await conn.execute("PRAGMA synchronous")
    row = await cur.fetchone()
    await cur.close()

    assert row is not None
    assert row[0] == 2  # 0=OFF, 1=NORMAL, 2=FULL, 3=EXTRA
    await conn.close()


# --- a durable timer survives a cold reopen and waits only the remainder ------


async def test_timer_survives_reopen_and_waits_remainder(tmp_path: Path) -> None:
    """A durable timer's deadline persists: reopen the file mid-sleep and resume.

    Process 1 records a 10s timer (deadline stamped from now=1000) and exits;
    process 2 opens the same file cold at now=1005 and waits only the 5s
    remainder. The deadline lived on disk, not in either process's memory.
    """
    path = str(tmp_path / "chronicle.db")
    conn1 = await aiosqlite.connect(path)
    log1 = await _open_log(conn1, "wf", asyncio.Lock())
    await run(
        sleep_once,
        (10.0,),
        log1,
        {},
        now=lambda: 1000.0,
        sleep=noop_sleep,  # record without waiting
    )
    await conn1.close()  # process 1 "dies" mid-sleep

    # Process 2 opens the same file cold, 5s before the recorded deadline.
    conn2 = await aiosqlite.connect(path)
    log2 = await _open_log(conn2, "wf", asyncio.Lock())
    clock = FakeClock()
    result = await run(
        sleep_once,
        (10.0,),
        log2,
        {},
        now=lambda: 1005.0,
        sleep=clock.sleep,
    )
    assert result == 1010.0
    assert clock.waits == [5.0]  # the persisted deadline, respected after a cold reopen
    assert len(await log2.replay()) == 1  # nothing re-recorded on resume
    await conn2.close()
