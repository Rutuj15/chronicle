"""The event-log seam: the append-only history the engine persists and replays.

The driver loop (``runtime.run``) talks *only* to the ``EventLog`` interface
defined here, never to a concrete store. That is the whole point of the seam:
an in-memory implementation and an aiosqlite-backed store both sit behind this
same interface, so swapping storage touches one module and leaves the replay
loop untouched.

The seam is **async**: ``run`` loads the recorded history once
(:meth:`EventLog.replay`) and awaits each new event's append
(:meth:`EventLog.append`). The engine runs on ``grpc.aio``, and a durable
append must reach disk before ``run`` proceeds (one fsync per event is the
durability boundary); awaiting it lets ``aiosqlite`` fsync on a background
thread, so the event loop stays unblocked. A sync append would either block the
loop on that fsync or never reach disk -- neither is acceptable on the engine.
The in-memory store implements the same async seam (no I/O, so the awaits are
trivial) and additionally keeps sync ``__len__``/``__getitem__``, so a test can
inspect a recorded history from a plain synchronous ``def`` without an event
loop.

This module owns *storage shape*, not serialization: an ``EventLog`` deals in
live ``Event`` objects. How those objects become bytes on disk is a separate
concern (``serialization.py``), layered in by the durable store.
"""

import asyncio
from collections.abc import Iterable
from typing import Protocol, cast

import aiosqlite

from .events import Event
from .serialization import dump_event, load_event


class EventLog(Protocol):
    """Append-only event history -- the persistence seam.

    The driver loop talks only to this interface, never to a concrete store.
    An in-memory implementation (``InMemoryEventLog``) and an
    aiosqlite-backed store (``SqliteEventLog``) both sit behind it, so swapping
    storage touches one module and leaves the loop untouched.

    Both methods are coroutines so a durable store can fsync *off* the event
    loop (the engine runs on ``grpc.aio``): a durable append must land on disk
    before ``run`` proceeds past it, and awaiting it parks the coroutine on the
    fsync rather than blocking the loop.
    """

    async def replay(self) -> list[Event]:
        """Load the whole recorded history in append order.

        Empty on a first run. ``run`` calls this once, up front, and drives the
        loop over the returned list -- so a durable store does a single batch
        read rather than one indexed lookup per command.
        """
        ...

    async def append(self, event: Event) -> None:
        """Durably record ``event`` as the next entry in the history.

        Under ``PRAGMA synchronous = FULL`` each call is one commit -- one
        fsync -- which is the durability boundary: the event is either fully on
        disk or not recorded at all.
        """
        ...


class InMemoryEventLog(EventLog):
    """A ``list``-backed event log -- the in-memory store.

    The async seam (``replay``/``append``) is what ``run`` drives. The sync
    ``__len__``/``__getitem__`` are an in-memory convenience, NOT part of the
    ``EventLog`` contract: they let a test inspect the recorded history after a
    synchronous ``run_sync`` without spinning up an event loop. They cost
    nothing because the store is already a Python list. ``replay`` returns a
    copy so ``run``'s local cursor and this store stay distinct objects.
    Construct with an iterable of events to seed a partial or reconstructed
    history -- the in-process stand-in for a durable log's replayed state.
    """

    def __init__(self, events: Iterable[Event] = ()) -> None:
        self._events: list[Event] = list(events)

    async def replay(self) -> list[Event]:
        return list(self._events)

    async def append(self, event: Event) -> None:
        self._events.append(event)

    def __len__(self) -> int:
        return len(self._events)

    def __getitem__(self, index: int) -> Event:
        return self._events[index]


class SqliteEventLog(EventLog):
    """An aiosqlite-backed event log that survives a real process restart.

    Each append is its own transaction under ``PRAGMA synchronous = FULL`` --
    which makes SQLite ``fsync`` before acknowledging the commit -- so an event
    is either fully on disk or not recorded at all. That line *is* the
    durability boundary:

    * crash before the commit -> the event is absent, so on resume the activity
      runs again (at-least-once execution);
    * crash after the commit -> the event is on disk, so on resume it is fed
      back from history and never re-executes.

    One fsync per event is deliberate: correct and explainable first, batched
    later. Storage is scoped by ``workflow_id`` -- one file holds many
    workflows' histories, keyed by ``(workflow_id, seq)`` where ``seq`` is the
    0-based append order, i.e. exactly the cursor ``run`` indexes the replayed
    history with. The schema is Postgres-portable.

    The connection is *injected and owned by the caller*; the log sets its own
    durability pragma and ensures its table in :meth:`start`, so the durability
    contract holds no matter how the connection was opened. Close the connection
    yourself when done.

    This is the async (``aiosqlite``) twin of the original sync log: the engine
    runs on ``grpc.aio``, so a durable append must fsync *off* the event loop.
    ``aiosqlite`` runs each SQL call on a background thread, so the commit parks
    this coroutine, not the loop. ``lock`` serializes multi-call sequences on
    the shared connection -- a ``replay`` SELECT cursor left open across an
    await would otherwise let a concurrent ``append``'s commit fail with
    "cannot commit transaction - SQL statements in progress" (the lesson the
    task queue learned the hard way). One lock per connection, shared by every
    log view over it; pass the same lock to every ``SqliteEventLog`` (and any
    other user) of one connection.
    """

    def __init__(
        self,
        conn: aiosqlite.Connection,
        workflow_id: str,
        lock: asyncio.Lock,
    ) -> None:
        self._conn = conn
        self._workflow_id = workflow_id
        self._lock = lock

    async def start(self) -> None:
        """Set the durability pragma and ensure the schema. Call once before use.

        Idempotent (``IF NOT EXISTS``), so it is safe for every log view over a
        shared connection to call it -- the first creates the table, the rest
        are no-ops.
        """
        # FULL = every commit fsyncs (the durability boundary). The default
        # rollback journal keeps the store to a single .db file after commit;
        # WAL is the task queue's choice (it has concurrent readers), not needed
        # here -- the lock serializes all access on this connection.
        async with self._lock:
            await self._conn.execute("PRAGMA synchronous = FULL")
            await self._conn.execute(
                "CREATE TABLE IF NOT EXISTS events ("
                " workflow_id TEXT NOT NULL,"
                " seq INTEGER NOT NULL,"
                " payload TEXT NOT NULL,"
                " PRIMARY KEY (workflow_id, seq)"
                ")"
            )
            await self._conn.commit()

    async def replay(self) -> list[Event]:
        """Load this workflow's recorded history in append (``seq`` order)."""
        async with self._lock:
            cur = await self._conn.execute(
                "SELECT payload FROM events WHERE workflow_id = ? ORDER BY seq",
                (self._workflow_id,),
            )
            rows = await cur.fetchall()
            await cur.close()
        return [load_event(row[0]) for row in rows]

    async def append(self, event: Event) -> None:
        """Persist ``event`` as the next entry -- one commit (one fsync) under FULL."""
        # seq is the next cursor position; holding the lock across the count and
        # the insert makes them atomic against a concurrent append on another
        # workflow's log view of the same connection.
        async with self._lock:
            cur = await self._conn.execute(
                "SELECT COUNT(*) FROM events WHERE workflow_id = ?",
                (self._workflow_id,),
            )
            row = await cur.fetchone()
            await cur.close()
            assert row is not None  # COUNT(*) always yields exactly one row
            seq = cast(int, row[0])
            await self._conn.execute(
                "INSERT INTO events (workflow_id, seq, payload) VALUES (?, ?, ?)",
                (self._workflow_id, seq, dump_event(event)),
            )
            await self._conn.commit()  # one commit per event == one fsync under FULL


__all__ = ["EventLog", "InMemoryEventLog", "SqliteEventLog"]
