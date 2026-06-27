"""End-to-end tests for the distributed Engine / Worker / Client.

These run a *real* ``grpc.aio`` server on a free loopback port with the Worker and
Client as async tasks in the same event loop -- not separate processes (that is
3c). The :func:`_cluster` context manager stands up the whole stack (durable
``SqliteTaskQueue`` on a temp DB, ``Engine`` servicer, ``run_worker`` task,
``Client``) and tears it back down, so each test reads as: start a workflow,
observe its result over gRPC.

What these pin:
* a workflow's activities run on the Worker and the Client observes the result;
* a terminal activity failure crosses the wire as one ``FAILED`` (the marker is
  reconstituted on the Engine -- the same failure model as in-process);
* retry/timeout/idempotency policy runs on the Worker (reusing ``_run_activity``);
* the engine-minted idempotency key ``"{workflow_id}:{seq}"`` is delivered to an
  idempotent activity and withheld from a non-idempotent one;
* a multi-activity workflow composes results across several Worker round-trips.
"""

import asyncio
import contextlib
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager

import aiosqlite
import grpc.aio

from chronicle.client import Client, WorkflowStatus
from chronicle.engine import Engine, WorkflowFn
from chronicle.proto import chronicle_pb2_grpc as pb_grpc
from chronicle.runtime import ActivityRegistry, ActivitySpec, AsyncSleeper, RetryPolicy
from chronicle.task_queue import SqliteTaskQueue
from chronicle.worker import run_worker
from conftest import noop_sleep


@asynccontextmanager
async def _cluster(
    workflows: Mapping[str, WorkflowFn],
    activities: ActivityRegistry,
    *,
    db_path: str,
    worker_sleep: AsyncSleeper = asyncio.sleep,
    poll_timeout: float = 0.2,
) -> AsyncIterator[Client]:
    """Stand up Engine + Worker + Client on one loop; yield the Client.

    Tears everything down in reverse on exit: cancel the Worker, stop the Engine's
    live workflows, stop the server, close the channel, close the task-queue
    connection. ``db_path`` should be a pytest ``tmp_path`` so SQLite's files are
    cleaned up automatically.
    """
    conn = await aiosqlite.connect(db_path)
    queue = SqliteTaskQueue(conn)
    await queue.start()
    engine = Engine(workflows, queue, poll_timeout=poll_timeout)

    server = grpc.aio.server()
    pb_grpc.add_ChronicleServicer_to_server(engine, server)
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()

    channel = grpc.aio.insecure_channel(f"127.0.0.1:{port}")
    worker_task = asyncio.create_task(
        run_worker(pb_grpc.ChronicleStub(channel), activities, sleep=worker_sleep)
    )
    try:
        yield Client(channel)
    finally:
        worker_task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await worker_task
        await engine.stop()
        await server.stop(grace=None)
        await channel.close()
        await conn.close()


async def test_workflow_runs_activity_on_worker_and_returns_result(tmp_path) -> None:
    # The happy path: the Client starts a workflow, its activity runs on the
    # Worker, and the composed result comes back over gRPC.
    async def greet(who: str) -> str:
        return f"Hello, {who}"

    async def greet_workflow(ctx, who: str) -> str:
        return await ctx.activity("greet", who)

    async with _cluster(
        {"greet": greet_workflow}, {"greet": greet}, db_path=str(tmp_path / "q.db")
    ) as client:
        await client.start_workflow("wf-1", "greet", "world")
        result = await client.get_result("wf-1", timeout=5.0)

    assert result.status is WorkflowStatus.COMPLETED
    assert result.result == "Hello, world"


async def test_failing_activity_surfaces_as_one_failed(tmp_path) -> None:
    # A terminal activity failure is reported by the Worker and reconstituted on
    # the Engine as one FAILED -- the same model as an in-process failure.
    async def boom() -> None:
        raise ValueError("kaboom")

    async def boom_workflow(ctx) -> None:
        await ctx.activity("boom")

    async with _cluster(
        {"boom": boom_workflow}, {"boom": boom}, db_path=str(tmp_path / "q.db")
    ) as client:
        await client.start_workflow("wf-2", "boom")
        result = await client.get_result("wf-2", timeout=5.0)

    assert result.status is WorkflowStatus.FAILED
    assert result.error_type == "ValueError"
    assert "kaboom" in (result.error_message or "")


async def test_retry_policy_runs_on_the_worker(tmp_path) -> None:
    # Retry lives next to the activity (on the Worker), reusing _run_activity: a
    # flaky activity retried under its policy succeeds, having run 3 times.
    attempts = 0

    async def flaky() -> str:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise RuntimeError("transient")
        return "ok"

    async def flaky_workflow(ctx) -> str:
        return await ctx.activity("flaky")

    spec = ActivitySpec(fn=flaky, retry=RetryPolicy(max_attempts=3, initial_backoff=0.0))

    async with _cluster(
        {"flaky": flaky_workflow},
        {"flaky": spec},
        db_path=str(tmp_path / "q.db"),
        worker_sleep=noop_sleep,  # no real backoff waiting
    ) as client:
        await client.start_workflow("wf-3", "flaky")
        result = await client.get_result("wf-3", timeout=5.0)

    assert result.status is WorkflowStatus.COMPLETED
    assert result.result == "ok"
    assert attempts == 3


async def test_idempotency_key_delivered_only_when_idempotent(tmp_path) -> None:
    # The Engine always mints "{workflow_id}:{seq}"; the Worker injects it as
    # idempotency_key= only when the local spec is idempotent. So the idempotent
    # activity sees wf-4:0 (seq 0) and the non-idempotent one sees no such kwarg.
    charge_key: str | None = None
    plain_kwargs: dict[str, object] = {}

    async def charge(amount: int, *, idempotency_key: str | None = None) -> dict[str, int]:
        nonlocal charge_key
        charge_key = idempotency_key
        return {"charged": amount}

    async def plain(x: int, **kwargs: object) -> int:
        plain_kwargs.update(kwargs)
        return x

    async def mixed_workflow(ctx) -> str:
        await ctx.activity("charge", 100)  # idempotent -> key injected (seq 0)
        await ctx.activity("plain", 42)  # not idempotent -> key withheld (seq 1)
        return "done"

    activities: ActivityRegistry = {
        "charge": ActivitySpec(fn=charge, idempotent=True),
        "plain": ActivitySpec(fn=plain),
    }
    async with _cluster(
        {"mixed": mixed_workflow}, activities, db_path=str(tmp_path / "q.db")
    ) as client:
        await client.start_workflow("wf-4", "mixed")
        result = await client.get_result("wf-4", timeout=5.0)

    assert result.status is WorkflowStatus.COMPLETED
    assert result.result == "done"
    assert charge_key == "wf-4:0"
    assert plain_kwargs == {}


async def test_multi_activity_workflow_composes_results(tmp_path) -> None:
    # Two activities compose across two Worker round-trips: the Engine drives seq
    # 0 and seq 1, the Worker runs each, and both results feed back into the
    # workflow -- the replay model intact over the wire.
    async def inc(x: int) -> int:
        return x + 1

    async def two_step_workflow(ctx) -> list[int]:
        first = await ctx.activity("inc", 1)
        second = await ctx.activity("inc", 2)
        return [first, second]

    async with _cluster(
        {"two_step": two_step_workflow}, {"inc": inc}, db_path=str(tmp_path / "q.db")
    ) as client:
        await client.start_workflow("wf-5", "two_step")
        result = await client.get_result("wf-5", timeout=5.0)

    assert result.status is WorkflowStatus.COMPLETED
    assert result.result == [2, 3]
