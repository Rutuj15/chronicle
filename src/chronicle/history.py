"""The event-log seam: the append-only history the engine persists and replays.

The driver loop (``runtime.run``) talks *only* to the ``EventLog`` interface
defined here, never to a concrete store. That is the whole point of the seam:
Week 1 ships one in-memory implementation, and Week 2 adds a SQLite-backed store
behind this same interface, so swapping storage touches one module and leaves
the replay loop untouched.

This module owns *storage shape*, not serialization: an ``EventLog`` deals in
live ``Event`` objects. How those objects become bytes on disk is a separate
concern (``serialization.py``), layered in by the durable store.
"""

import sqlite3
from typing import Protocol, cast

from .events import Event
from .serialization import dump_event, load_event


class EventLog(Protocol):
    """Append-only event history -- the persistence seam.

    The driver loop talks only to this interface, never to a concrete store.
    Week 1 has one implementation (``InMemoryEventLog``); Week 2 adds a
    SQLite-backed store behind this same interface, so swapping storage touches
    one module and leaves the loop untouched.
    """

    def append(self, event: Event) -> None: ...

    def __len__(self) -> int: ...

    def __getitem__(self, index: int) -> Event: ...


class InMemoryEventLog(EventLog):
    """A ``list``-backed event log -- Week 1's only store."""

    def __init__(self) -> None:
        self._events: list[Event] = []

    def append(self, event: Event) -> None:
        self._events.append(event)

    def __len__(self) -> int:
        return len(self._events)

    def __getitem__(self, index: int) -> Event:
        return self._events[index]


class SqliteEventLog(EventLog):
    """A SQLite-backed event log that survives a real process restart.

    Each append is its own transaction, committed under ``PRAGMA synchronous =
    FULL`` -- which makes SQLite ``fsync`` before acknowledging the commit. An
    event is therefore either fully on disk or not recorded at all, and that line
    *is* the durability boundary:

    * crash or exit *before* the commit -> the event is absent, so on resume the
      activity runs again (at-least-once execution);
    * crash or exit *after* the commit -> the event is on disk, so on resume it
      is fed back from history and never re-executes.

    One fsync per event is deliberate: correct and explainable first, batched
    later. Storage is scoped by ``workflow_id`` -- one
    file holds many workflows' histories, keyed by ``(workflow_id, seq)`` where
    ``seq`` is the 0-based append order, i.e. exactly the cursor the replay loop
    indexes with. The schema is Postgres-portable for Week 5.

    The connection is *injected and owned by the caller*; the log sets its own
    durability pragma and ensures the schema, so the durability contract holds
    no matter how the connection was opened. Close the connection yourself when
    done.
    """

    def __init__(self, conn: sqlite3.Connection, workflow_id: str) -> None:
        self._conn = conn
        self._workflow_id = workflow_id
        # FULL = every commit fsyncs (the durability boundary). The default
        # rollback journal keeps the store to a single .db file after commit --
        # the simplest possible "the log is this one file" story. WAL is the
        # Week 5 choice, when concurrent readers appear.
        conn.execute("PRAGMA synchronous = FULL")
        conn.execute(
            "CREATE TABLE IF NOT EXISTS events ("
            " workflow_id TEXT NOT NULL,"
            " seq INTEGER NOT NULL,"
            " payload TEXT NOT NULL,"
            " PRIMARY KEY (workflow_id, seq)"
            ")"
        )
        conn.commit()

    def append(self, event: Event) -> None:
        # seq is the next cursor position; single writer, so it equals the count.
        seq = len(self)
        self._conn.execute(
            "INSERT INTO events (workflow_id, seq, payload) VALUES (?, ?, ?)",
            (self._workflow_id, seq, dump_event(event)),
        )
        self._conn.commit()  # one commit per event == one fsync under FULL

    def __len__(self) -> int:
        cur = self._conn.execute(
            "SELECT COUNT(*) FROM events WHERE workflow_id = ?",
            (self._workflow_id,),
        )
        row = cur.fetchone()
        assert row is not None  # COUNT(*) always yields exactly one row
        return cast(int, row[0])

    def __getitem__(self, index: int) -> Event:
        cur = self._conn.execute(
            "SELECT payload FROM events WHERE workflow_id = ? AND seq = ?",
            (self._workflow_id, index),
        )
        row = cur.fetchone()
        if row is None:
            raise IndexError(index)
        return load_event(row[0])


__all__ = ["EventLog", "InMemoryEventLog", "SqliteEventLog"]
