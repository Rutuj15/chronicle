"""The durable activity-task queue: how the Engine hands work to Workers.

The Engine owns workflow replay and coordination; Workers own only the activity
code. A task queue is the bridge between the two halves of the executor seam:
whenever a parked workflow coroutine issues an activity command, the Engine
*enqueues* one unit of activity work; a Worker *long-polls* to take the next
unit, runs it, and reports the outcome back over a separate RPC.

This is the *coordination* store, deliberately distinct from the Engine's
per-workflow event log (which in 3b lives in memory). The task queue is durable
on purpose -- it is the foundation task leasing (3c) and worker crash recovery
(Week 6) build on -- so each enqueue and each completion is its own commit under
WAL + ``synchronous = FULL`` (one fsync), exactly the durability boundary the
event log uses. An enqueued task is on disk before the Engine's parked coroutine
is handed back, so a crash between enqueue and execution redelivers the task
rather than dropping it.

**Task leasing (a visibility timeout -- the SQS / Temporal model).** A taken
task is NOT deleted; it is *leased*. Each row carries a ``visible_at`` deadline:
while ``visible_at > now`` the task is hidden from other pollers (one worker owns
it); once the lease expires it becomes visible again and another worker may take
it over. Three operations move a task through its lifecycle:

* :meth:`enqueue` inserts a row that is immediately visible (``visible_at = now``).
* :meth:`poll` claims the oldest *visible* row and pushes its invisibility out by
  the lease (``visible_at = now + lease``, ``attempts += 1``) -- one atomic
  ``UPDATE ... RETURNING``, so two pollers can never take the same task.
* :meth:`complete` deletes the row -- the durable signal a worker finished. The
  Engine calls this on ``ReportActivityResult``, so a finished task is never
  redelivered.
* :meth:`release` makes a row visible again (``visible_at = now + retry_after``)
  -- a worker *nack*. The Engine calls this on ``ReleaseActivityTask`` when a
  worker cannot action a task (e.g. a missing activity mid rolling-deploy), so
  another worker gets a shot instead of waiting for the lease to expire.

Why this fixes the 3b wedge: if a worker dies (or ``KeyError``s) after claiming a
task, ``complete`` never fires; the lease expires; the next ``poll`` re-exposes
the row and a second worker runs it. Redelivery returns the *same row* -- hence
the same ``task_id``, which keys the Engine's parked ``asyncio.Future`` -- so
whichever delivery reports first resumes the workflow and a late duplicate is a
no-op. The activity may run twice (at-least-once), which is exactly why the
Engine mints idempotency keys (Week 4). No new failure model touches the replay
loop: ``attempts`` is delivery-layer metadata, never part of the command or
event, so the determinism guard is untouched.

Reaping is *lazy* -- there is no background sweeper. An expired lease is simply
observed by the next ``poll`` (``visible_at <= now`` in the claim query), so
redelivery latency is bounded by how often workers poll, not by a sweep cadence.
Long-polling -- blocking a poll until work appears or a budget expires -- rides
on an :class:`asyncio.Condition` that ``enqueue``/``release`` notify after every
commit; ``poll`` re-checks the queue, then waits on the condition for the
remaining budget, looping so that a notification that arrived just before the
wait (a "lost wakeup") is still observed.
"""

import asyncio
import time
from collections.abc import Callable
from typing import Protocol

import aiosqlite

from chronicle.proto.chronicle_pb2 import ActivityTask

# The clock the queue reads for lease deadlines. Monotonic (not wall-clock):
# lease *durations* are relative, so immunity to a wall-clock jump matters more
# than sharing a timeline with workflow timers. Injected so a test advances a
# fake clock and observes lease expiry instantly and deterministically.
Clock = Callable[[], float]


class TaskQueue(Protocol):
    """The coordination seam between Engine (producer) and Worker (consumer).

    The Engine holds a ``TaskQueue`` and enqueues an :class:`ActivityTask` for
    each activity a parked workflow issues; a Worker long-polls to consume them.
    Leasing adds :meth:`complete` (a worker finished -- stop redelivering) and
    :meth:`release` (a worker declined -- re-queue for another). A concrete
    implementation (``SqliteTaskQueue``) sits behind this interface, so the
    durability strategy is swappable without touching the Engine or Worker.
    """

    async def enqueue(self, task: ActivityTask) -> None:
        """Append ``task`` durably (immediately visible); wake parked pollers."""
        ...

    async def poll(self, *, timeout: float) -> ActivityTask | None:
        """Claim the next visible task, blocking up to ``timeout`` seconds.

        Extends the claimed task's lease so other pollers can't take it. Returns
        the task, or ``None`` if the budget expired with nothing visible (a
        worker then re-polls).
        """
        ...

    async def complete(self, task_id: str) -> None:
        """Delete a finished task -- the durable signal its lease is satisfied.

        Idempotent: a no-op if the task was already completed by another
        delivery. Called by the Engine on ``ReportActivityResult``.
        """
        ...

    async def release(self, task_id: str, *, retry_after: float = 0.0) -> None:
        """Re-queue a task a worker declined -- make it visible to others.

        Called by the Engine on ``ReleaseActivityTask`` (a worker nack, e.g. a
        missing activity). ``retry_after`` seconds until re-delivery (0 = now).
        Idempotent: a no-op if the task is already gone.
        """
        ...


class SqliteTaskQueue:
    """A durable, leased activity-task queue backed by SQLite (via ``aiosqlite``).

    The connection is *injected and owned by the caller*: pass an open
    :class:`aiosqlite.Connection`, then :meth:`start` it once to set the
    durability pragmas and ensure the schema. Close the connection yourself when
    done. This mirrors :class:`chronicle.core.history.SqliteEventLog` -- the other
    SQLite seam -- and keeps the queue decoupled from how the connection (and
    its DB path) was opened.

    ``aiosqlite`` (not the sync ``sqlite3`` module) keeps the ``grpc.aio`` event
    loop unblocked: every SQL call awaits a background thread, so an fsync on
    commit parks this coroutine, not the loop.

    ``lease_seconds`` is the visibility timeout: how long a claimed task stays
    hidden from other pollers. ``now`` is the clock the lease is read against
    (default :func:`time.monotonic`); tests inject a controllable clock so lease
    expiry is instant and deterministic.
    """

    def __init__(
        self,
        conn: aiosqlite.Connection,
        *,
        lease_seconds: float = 30.0,
        now: Clock = time.monotonic,
    ) -> None:
        # A non-positive lease would re-expose a task the instant it is claimed,
        # redelivering forever -- a nonsensical config, so fail loud at build.
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be > 0")
        self._conn = conn
        self._lease = lease_seconds
        self._now = now
        # The condition is the long-poll wakeup channel AND the single mutation
        # lock for this connection. aiosqlite serializes individual SQL calls on
        # its worker thread but NOT multi-call sequences -- a RETURNING cursor
        # left open across awaits (in _claim) would make a concurrent commit
        # fail with "SQL statements in progress". So every queue mutation
        # (enqueue's INSERT+commit, _claim's UPDATE+fetch+close, complete's
        # DELETE+commit, release's UPDATE+commit) runs while holding this lock,
        # keeping each sequence atomic on the shared conn.
        self._cond = asyncio.Condition()

    async def start(self) -> None:
        """Configure durability and ensure the schema. Call once before use."""
        # WAL: one writer, concurrent readers -- the journal for a queue that
        # may be inspected for an in-flight lease while the Engine writes.
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
            " idempotency_key TEXT NOT NULL,"  # "{workflow_id}:{seq}", engine-minted
            " visible_at REAL NOT NULL,"  # lease deadline; <= now means visible
            " attempts INTEGER NOT NULL DEFAULT 0"  # delivery count (observability)
            ")"
        )
        await self._conn.commit()

    async def enqueue(self, task: ActivityTask) -> None:
        """Persist ``task`` (one fsync) immediately visible; wake parked pollers.

        ``visible_at = now`` so a freshly enqueued task is claimable right away;
        ``attempts`` starts at 0 and becomes 1 on the first claim.
        """
        now = self._now()
        # INSERT+commit run under the condition lock so they cannot interleave
        # with a concurrent _claim's open RETURNING cursor (see _cond). Notify
        # only after the commit, so a woken poller is guaranteed to see the row.
        async with self._cond:
            await self._conn.execute(
                "INSERT INTO tasks (task_id, workflow_id, activity_name, args_json,"
                " idempotency_key, visible_at, attempts)"
                " VALUES (?, ?, ?, ?, ?, ?, 0)",
                (
                    task.task_id,
                    task.workflow_id,
                    task.activity_name,
                    task.args_json,
                    task.idempotency_key,
                    now,  # immediately visible
                ),
            )
            await self._conn.commit()
            self._cond.notify_all()

    async def poll(self, *, timeout: float) -> ActivityTask | None:
        """Claim the oldest visible task, blocking up to ``timeout`` seconds.

        Re-checks the queue, then waits on the condition for the remaining
        budget, looping -- so a notification that arrived in the gap before the
        wait is still observed on the next re-check rather than stalling the
        poller for the whole budget. Returns ``None`` on an empty timeout.
        """
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        async with self._cond:
            while True:
                task = await self._claim()
                if task is not None:
                    return task
                remaining = deadline - loop.time()
                if remaining <= 0:
                    return None
                # cond.wait() releases the lock while parked (another poller or
                # an enqueue/release can run) and reacquires it on wake; the
                # budget bounds the whole wait so the worker re-polls promptly.
                try:
                    await asyncio.wait_for(self._cond.wait(), remaining)
                except TimeoutError:
                    return None

    async def _claim(self) -> ActivityTask | None:
        """Atomically claim the oldest visible task and extend its lease.

        A single ``UPDATE ... RETURNING`` selects the oldest row whose lease has
        expired (``visible_at <= now``), extends its invisibility by the lease,
        bumps ``attempts``, and returns it -- so two concurrent pollers can never
        claim the same task. Returns ``None`` when nothing is visible right now
        (every task in-flight under a live lease, or the queue empty). Reaping
        is lazy: this query *is* the reaper -- an expired lease is simply a row
        whose ``visible_at`` has slipped into the past.
        """
        now = self._now()
        cur = await self._conn.execute(
            "UPDATE tasks"
            " SET visible_at = ?, attempts = attempts + 1"
            " WHERE id = (SELECT id FROM tasks WHERE visible_at <= ? ORDER BY id LIMIT 1)"
            " RETURNING task_id, workflow_id, activity_name, args_json,"
            " idempotency_key, attempts",
            (now + self._lease, now),
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
            attempts=row[5],
        )

    async def complete(self, task_id: str) -> None:
        """Delete a finished task -- the durable signal its lease is satisfied.

        Idempotent: a stale duplicate report (a faster delivery already completed
        the task) finds no row and does nothing. The Engine calls this on
        ``ReportActivityResult`` so a finished task is never redelivered.
        """
        async with self._cond:
            await self._conn.execute("DELETE FROM tasks WHERE task_id = ?", (task_id,))
            await self._conn.commit()

    async def release(self, task_id: str, *, retry_after: float = 0.0) -> None:
        """Re-queue a task a worker declined -- make it visible to others again.

        The Engine calls this on ``ReleaseActivityTask`` (a worker nack, e.g. a
        missing activity during a rolling deploy). The task becomes visible to
        other pollers after ``retry_after`` seconds (``0`` = immediately).
        Idempotent: a no-op if the task was already completed by another
        delivery. Notifying wakes a parked poller so redelivery is prompt.
        """
        now = self._now()
        async with self._cond:
            await self._conn.execute(
                "UPDATE tasks SET visible_at = ? WHERE task_id = ?",
                (now + retry_after, task_id),
            )
            await self._conn.commit()
            self._cond.notify_all()


__all__ = ["SqliteTaskQueue", "TaskQueue"]
