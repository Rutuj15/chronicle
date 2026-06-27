"""The durable activity-task queue: how the Engine hands work to Workers.

The Engine owns workflow replay and coordination; Workers own only the activity
code. A task queue is the bridge between the two halves of the executor seam:
whenever a parked workflow coroutine issues an activity command, the Engine
*enqueues* one unit of activity work; a Worker *long-polls* to take the next
unit, runs it, and reports the outcome back over a separate RPC.

This is the *coordination* store, deliberately distinct from the Engine's
per-workflow event log (which in 3b lives in memory). The task queue is durable
on purpose -- it is the foundation  (task leasing / redelivery)
(worker crash recovery) build on -- so each enqueue is its own commit under WAL
+ ``synchronous = FULL`` (one fsync), exactly the durability boundary the event
log uses. An enqueued task is on disk before the Engine's parked coroutine is
handed back, so a crash between enqueue and execution redelivers the task rather
than dropping it.

The queue is strict FIFO. Dequeue is a single ``DELETE ... RETURNING``
statement: SQLite selects the oldest row (lowest autoincrement ``id``), deletes
it, and hands it back atomically -- so two concurrent pollers can never take the
same task. Long-polling -- blocking a poll until work appears or a budget
expires -- rides on an :class:`asyncio.Condition` that ``enqueue`` notifies
after every commit; ``poll`` re-checks the queue, then waits on the condition
for the remaining budget, looping so that a notification that arrived just
before the wait (a "lost wakeup") is still observed.
"""

import asyncio
from typing import Protocol

import aiosqlite

from .proto.chronicle_pb2 import ActivityTask


class TaskQueue(Protocol):
    """The coordination seam between Engine (producer) and Worker (consumer).

    The Engine holds a ``TaskQueue`` and enqueues an :class:`ActivityTask` for
    each activity a parked workflow issues; a Worker long-polls to consume them.
    A concrete implementation (``SqliteTaskQueue``) sits behind this interface,
    so the durability strategy is swappable without touching the Engine or
    Worker.
    """

    async def enqueue(self, task: ActivityTask) -> None:
        """Append ``task`` durably; wake any worker parked in :meth:`poll`."""
        ...

    async def poll(self, *, timeout: float) -> ActivityTask | None:
        """Take the next task, blocking up to ``timeout`` seconds for one to appear.

        Returns the task, or ``None`` if the budget expired with the queue empty
        (a worker then re-polls).
        """
        ...


class SqliteTaskQueue:
    """A durable, FIFO activity-task queue backed by SQLite (via ``aiosqlite``).

    The connection is *injected and owned by the caller*: pass an open
    :class:`aiosqlite.Connection`, then :meth:`start` it once to set the
    durability pragmas and ensure the schema. Close the connection yourself when
    done. This mirrors :class:`chronicle.history.SqliteEventLog` -- the other
    SQLite seam -- and keeps the queue decoupled from how the connection (and
    its DB path) was opened.

    ``aiosqlite`` (not the sync ``sqlite3`` module) keeps the ``grpc.aio`` event
    loop unblocked: every SQL call awaits a background thread, so an fsync on
    commit parks this coroutine, not the loop.
    """

    def __init__(self, conn: aiosqlite.Connection) -> None:
        self._conn = conn
        # The condition is the long-poll wakeup channel AND the single mutation
        # lock for this connection. aiosqlite serializes individual SQL calls on
        # its worker thread but NOT multi-call sequences -- a RETURNING cursor
        # left open across awaits (in _take_one) would make a concurrent commit
        # fail with "SQL statements in progress". So every queue mutation
        # (enqueue's INSERT+commit, _take_one's DELETE+fetch+close) runs while
        # holding this lock, keeping each sequence atomic on the shared conn.
        self._cond = asyncio.Condition()

    async def start(self) -> None:
        """Configure durability and ensure the schema. Call once before use."""
        # WAL: one writer, concurrent readers -- the journal for a queue that
        # may be inspected for leases while the Engine writes.
        # FULL synchronous: every commit fsyncs, so an enqueued task is durable
        # before the Engine resumes -- the delivery durability boundary.
        await self._conn.execute("PRAGMA journal_mode = WAL")
        await self._conn.execute("PRAGMA synchronous = FULL")
        await self._conn.execute(
            "CREATE TABLE IF NOT EXISTS tasks ("
            " id INTEGER PRIMARY KEY AUTOINCREMENT,"  # insertion order -> FIFO
            " task_id TEXT NOT NULL,"  # engine-minted; keys the parked Future
            " workflow_id TEXT NOT NULL,"
            " activity_name TEXT NOT NULL,"
            " args_json TEXT NOT NULL,"  # JSON tuple, wire-faithful
            " idempotency_key TEXT NOT NULL"  # "{workflow_id}:{seq}", engine-minted
            ")"
        )
        await self._conn.commit()

    async def enqueue(self, task: ActivityTask) -> None:
        """Persist ``task`` (one fsync) and wake any worker parked in :meth:`poll`."""
        # INSERT+commit run under the condition lock so they cannot interleave
        # with a concurrent _take_one's open RETURNING cursor (see _cond). Notify
        # only after the commit, so a woken poller is guaranteed to see the row.
        async with self._cond:
            await self._conn.execute(
                "INSERT INTO tasks (task_id, workflow_id, activity_name, args_json,"
                " idempotency_key) VALUES (?, ?, ?, ?, ?)",
                (
                    task.task_id,
                    task.workflow_id,
                    task.activity_name,
                    task.args_json,
                    task.idempotency_key,
                ),
            )
            await self._conn.commit()
            self._cond.notify_all()

    async def poll(self, *, timeout: float) -> ActivityTask | None:
        """Take the oldest task, blocking up to ``timeout`` seconds for one.

        Re-checks the queue, then waits on the condition for the remaining
        budget, looping -- so a notification that arrived in the gap before the
        wait is still observed on the next re-check rather than stalling the
        poller for the whole budget. Returns ``None`` on an empty timeout.
        """
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        async with self._cond:
            while True:
                task = await self._take_one()
                if task is not None:
                    return task
                remaining = deadline - loop.time()
                if remaining <= 0:
                    return None
                # cond.wait() releases the lock while parked (another poller or
                # an enqueue can run) and reacquires it on wake; the budget
                # bounds the whole wait so the worker re-polls promptly.
                try:
                    await asyncio.wait_for(self._cond.wait(), remaining)
                except TimeoutError:
                    return None

    async def _take_one(self) -> ActivityTask | None:
        """Atomically delete and return the oldest task, or ``None`` if empty.

        A single ``DELETE ... RETURNING`` makes the take atomic: SQLite selects
        the oldest row, deletes it, and returns it in one statement, so there is
        no window in which two pollers could select the same row.
        """
        cur = await self._conn.execute(
            "DELETE FROM tasks WHERE id = (SELECT MIN(id) FROM tasks)"
            " RETURNING task_id, workflow_id, activity_name, args_json,"
            " idempotency_key"
        )
        row = await cur.fetchone()
        await cur.close()
        if row is None:
            return None
        return ActivityTask(
            task_id=row[0],
            workflow_id=row[1],
            activity_name=row[2],
            args_json=row[3],
            idempotency_key=row[4],
        )


__all__ = ["SqliteTaskQueue", "TaskQueue"]
