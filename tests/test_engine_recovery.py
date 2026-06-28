"""Week 6: an engine crash no longer loses in-flight workflows.

The engine's per-workflow event log is durable, and on a restart ``recover()``
replays each workflow's recorded history into a fresh live ``run`` coroutine --
resuming exactly where it left off. These tests prove it by standing up an
engine, driving a workflow (partway or to completion), tearing the engine down,
and building a *fresh* engine over the same durable files: the workflow is
reconstructed from history alone.

The logical crash (discard the engine object + its connections, keep the files)
is the in-process stand-in for a process death -- the same shape as the Week-1/2
durability tests. A real kill-the-engine-process demo is a follow-up slice.
"""

import asyncio
import contextlib
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from pathlib import Path

import aiosqlite
import grpc.aio

from chronicle.client import Client, WorkflowStatus
from chronicle.core.runtime import ActivityRegistry, AsyncSleeper
from chronicle.proto import chronicle_pb2_grpc as pb_grpc
from chronicle.server.engine import Engine, WorkflowFn
from chronicle.server.task_queue import SqliteTaskQueue
from chronicle.worker import run_worker


@asynccontextmanager
async def _engine(
    workflows: Mapping[str, WorkflowFn],
    *,
    queue_db: str,
    events_db: str,
    activities: ActivityRegistry | None = None,
    poll_timeout: float = 0.2,
    result_timeout: float = 5.0,
    lease_seconds: float = 30.0,
    worker_sleep: AsyncSleeper = asyncio.sleep,
) -> AsyncIterator[Client]:
    """One Engine (and an optional Worker) over durable DB paths.

    The two stores live in the given files, which PERSIST across instances: a
    second ``_engine`` opened on the same paths recovers the first's state.
    Everything in memory (the engine, the server, the connections) is torn down
    on exit; only the files remain.
    """
    queue_conn = await aiosqlite.connect(queue_db)
    queue = SqliteTaskQueue(queue_conn, lease_seconds=lease_seconds)
    await queue.start()
    event_conn = await aiosqlite.connect(events_db)
    engine = Engine(
        workflows, queue, event_conn, poll_timeout=poll_timeout, result_timeout=result_timeout
    )
    await engine.start()  # recover any workflows persisted by a prior engine

    server = grpc.aio.server()
    pb_grpc.add_ChronicleServicer_to_server(engine, server)
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()

    channel = grpc.aio.insecure_channel(f"127.0.0.1:{port}")
    worker_task: asyncio.Task[None] | None = None
    if activities is not None:
        worker_task = asyncio.create_task(
            run_worker(pb_grpc.ChronicleStub(channel), activities, sleep=worker_sleep)
        )
    try:
        yield Client(channel)
    finally:
        if worker_task is not None:
            worker_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await worker_task
        await engine.stop()
        await server.stop(grace=None)
        await channel.close()
        await event_conn.close()
        await queue_conn.close()


# --- a workflow + activities shared across the recovery tests ------------------

_calls: dict[str, int] = {"greet": 0, "shout": 0}


async def _greet(name: str) -> str:
    _calls["greet"] += 1
    return f"hello {name}"


async def _shout(text: str) -> str:
    _calls["shout"] += 1
    return text.upper()


async def two_step(ctx, name: str) -> str:
    greeting = await ctx.activity("greet", name)
    shouted = await ctx.activity("shout", greeting)
    return f"{greeting} >>> {shouted}"


# --- the headline: a completed workflow is reconstructed by pure replay --------


async def test_completed_workflow_is_reconstructed_by_replay(tmp_path: Path) -> None:
    """Engine 1 runs a workflow to completion; engine 2, cold over the same files,
    reconstructs it from history -- with NO worker attached.

    No worker can run activities on engine 2, so the only way ``get_result``
    returns COMPLETED is pure replay: the recorded history is fed back, never
    re-executed. Had recover() tried to re-run an activity, it would park forever
    (no worker) and time out as RUNNING.
    """
    _calls["greet"] = _calls["shout"] = 0
    queue_db = str(tmp_path / "q.db")
    events_db = str(tmp_path / "q.db.events")

    # Engine 1: run the two-activity workflow to completion, with a worker.
    async with _engine(
        {"two_step": two_step},
        queue_db=queue_db,
        events_db=events_db,
        activities={"greet": _greet, "shout": _shout},
    ) as client1:
        await client1.start_workflow("wf", "two_step", "world")
        before = await client1.get_result("wf", timeout=5.0)
    assert before.status is WorkflowStatus.COMPLETED
    assert before.result == "hello world >>> HELLO WORLD"
    assert _calls == {"greet": 1, "shout": 1}  # both ran, once, on engine 1

    # Engine 1 is gone; only its durable files survive.

    # Engine 2: cold open the same files, NO worker. start() recovers "wf" by
    # replaying its history; the reconstructed coroutine finishes at once.
    async with _engine({"two_step": two_step}, queue_db=queue_db, events_db=events_db) as client2:
        after = await client2.get_result("wf", timeout=5.0)
    assert after.status is WorkflowStatus.COMPLETED
    assert after.result == "hello world >>> HELLO WORLD"
    assert _calls == {"greet": 1, "shout": 1}  # replay ran neither again


# --- a crash mid-activity: replay the done step, resume the in-flight one ------


async def test_workflow_resumes_an_in_flight_activity_after_restart(tmp_path: Path) -> None:
    """Engine 1 crashes while an activity is in flight; engine 2 replays the
    completed step (it does NOT re-run) and resumes the in-flight one.

    ``shout`` blocks on ``proceed`` so engine 1 is torn down while shout is
    in-flight -- claimed but never reported, so no event for it was recorded. On
    engine 2, ``recover`` replays ``greet`` from history, then crosses into new
    ground at ``shout`` and re-issues it. The task engine 1 left in the queue
    survives too, and its lease is *in-process* (the ``_claim`` UPDATE is never
    committed), so a fresh engine sees it as immediately visible -- the stale
    task redelivers right away, alongside the recovered re-issue. So ``shout``
    runs at-least-once, which is exactly the contract the engine-minted
    idempotency keys make safe. What this test pins: ``greet`` was replayed
    (never re-run) and the workflow completed.
    """
    _calls["greet"] = _calls["shout"] = 0
    started = asyncio.Event()
    proceed = asyncio.Event()

    async def blocking_shout(text: str) -> str:
        started.set()
        await proceed.wait()  # hold the activity in-flight until the test releases it
        _calls["shout"] += 1
        return text.upper()

    queue_db = str(tmp_path / "q.db")
    events_db = str(tmp_path / "q.db.events")

    # Engine 1: greet completes (event recorded); shout is claimed and blocks.
    async with _engine(
        {"two_step": two_step},
        queue_db=queue_db,
        events_db=events_db,
        activities={"greet": _greet, "shout": blocking_shout},
    ) as client1:
        await client1.start_workflow("wf", "two_step", "world")
        await started.wait()  # engine 1's worker is inside shout, in-flight
    # Engine 1 torn down: shout never reported; its task is leased + hidden.

    proceed.set()  # release the resumed shout the moment engine 2 runs it
    async with _engine(
        {"two_step": two_step},
        queue_db=queue_db,
        events_db=events_db,
        activities={"greet": _greet, "shout": blocking_shout},
    ) as client2:
        after = await client2.get_result("wf", timeout=5.0)

    assert after.status is WorkflowStatus.COMPLETED
    assert after.result == "hello world >>> HELLO WORLD"
    assert _calls["greet"] == 1  # ran once on engine 1, REPLAYED (not re-run) on engine 2
    assert _calls["shout"] >= 1  # resumed -- at-least-once (stale redelivery + re-issue)


# --- a failed workflow reconstructs to the same failed state -------------------


async def test_failed_workflow_is_reconstructed_as_failed(tmp_path: Path) -> None:
    """A terminally-failed workflow is reconstructed to the SAME failed state.

    The recorded ``Failed`` event replays and re-raises, so ``get_result`` returns
    FAILED -- with no worker attached, which only pure replay of the failure can
    do (no worker could re-run the activity to fail again).
    """
    queue_db = str(tmp_path / "q.db")
    events_db = str(tmp_path / "q.db.events")

    async def boom() -> None:
        raise ValueError("kaboom")

    async def boom_workflow(ctx) -> None:
        await ctx.activity("boom")

    # Engine 1: the activity fails terminally -> one Failed event -> FAILED.
    async with _engine(
        {"boom": boom_workflow},
        queue_db=queue_db,
        events_db=events_db,
        activities={"boom": boom},
    ) as client1:
        await client1.start_workflow("wf", "boom")
        before = await client1.get_result("wf", timeout=5.0)
    assert before.status is WorkflowStatus.FAILED
    assert before.error_type == "ValueError"

    # Engine 2: no worker. recover() replays the Failed event -> re-raises -> FAILED.
    async with _engine({"boom": boom_workflow}, queue_db=queue_db, events_db=events_db) as client2:
        after = await client2.get_result("wf", timeout=5.0)
    assert after.status is WorkflowStatus.FAILED
    assert after.error_type == "ValueError"
