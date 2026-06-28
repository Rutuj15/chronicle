"""End-to-end tests for the distributed Engine / Worker / Client.

These run a *real* ``grpc.aio`` server on a free loopback port with the Worker and
Client as async tasks in the same event loop. (Real separate processes are the
3c demo in ``examples/``; these tests pin the behaviour, cheaply and
deterministically, in one loop.) :func:`_server` stands up Engine + Client and
hands back a worker factory so a test can start workers (and kill them)
when it chooses; :func:`_cluster` is the single-worker convenience over it.

What these pin:
* a workflow's activities run on the Worker and the Client observes the result;
* a terminal activity failure crosses the wire as one ``FAILED`` (the marker is
  reconstituted on the Engine -- the same failure model as in-process);
* retry/timeout/idempotency policy runs on the Worker (reusing ``_run_activity``);
* the engine-minted idempotency key ``"{workflow_id}:{seq}"`` is delivered to an
  idempotent activity and withheld from a non-idempotent one;
* a multi-activity workflow composes results across several Worker round-trips;
* task leasing -- a worker that claims a task and dies does not wedge the
  workflow: its lease expires, the task redelivers, another worker completes it;
* a missing activity on one worker is nacked (released) and redelivered to one
  that has it, rather than bricking the worker or wedging the workflow;
* GetWorkflowResult long-polls -- an in-flight workflow surfaces as RUNNING,
  then COMPLETED once it finishes.
"""

import asyncio
import contextlib
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from contextlib import asynccontextmanager

import aiosqlite
import grpc.aio

from chronicle.client import Client, WorkflowStatus
from chronicle.engine import Engine, WorkflowFn
from chronicle.proto import chronicle_pb2_grpc as pb_grpc
from chronicle.runtime import (
    ActivityRegistry,
    ActivitySpec,
    AsyncSleeper,
    Clock,
    RetryPolicy,
)
from chronicle.task_queue import SqliteTaskQueue
from chronicle.worker import run_worker
from conftest import noop_sleep

# A factory a test calls to start a worker on the shared channel; returns the
# worker task so the test can await/cancel it. _server owns teardown.
WorkerStarter = Callable[[ActivityRegistry], Awaitable[asyncio.Task[None]]]


class _MonotonicFake:
    """A controllable stand-in for ``time.monotonic``, for deterministic leases.

    The task queue reads this clock for lease deadlines; advancing it past the
    lease makes an in-flight task reappear instantly, with no real wall-clock
    waiting.
    """

    def __init__(self, start: float = 1000.0) -> None:
        self.t = start

    def __call__(self) -> float:
        return self.t

    def advance(self, dt: float) -> None:
        self.t += dt


@asynccontextmanager
async def _server(
    workflows: Mapping[str, WorkflowFn],
    *,
    db_path: str,
    poll_timeout: float = 0.2,
    result_timeout: float = 5.0,
    lease_seconds: float = 30.0,
    now: Clock = time.monotonic,
    worker_sleep: AsyncSleeper = asyncio.sleep,
    nack_retry_after: float = 0.0,
) -> AsyncIterator[tuple[Client, WorkerStarter]]:
    """Stand up Engine + Client (no workers); yield the client and a worker factory.

    Lets a test start workers when it chooses -- and cancel one mid-flight -- the
    leverage the leasing and missing-activity tests need. ``now`` is the queue's
    lease clock (default real monotonic; pass a :class:`_MonotonicFake` to expire
    a lease instantly). Tears the stack down on exit (every started worker,
    engine, server, channel, queue connection).
    """
    conn = await aiosqlite.connect(db_path)
    queue = SqliteTaskQueue(conn, lease_seconds=lease_seconds, now=now)
    await queue.start()
    engine = Engine(
        workflows, queue, poll_timeout=poll_timeout, result_timeout=result_timeout
    )

    server = grpc.aio.server()
    pb_grpc.add_ChronicleServicer_to_server(engine, server)
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()

    channel = grpc.aio.insecure_channel(f"127.0.0.1:{port}")
    worker_tasks: list[asyncio.Task[None]] = []

    async def make_worker(registry: ActivityRegistry) -> asyncio.Task[None]:
        task = asyncio.create_task(
            run_worker(
                pb_grpc.ChronicleStub(channel),
                registry,
                sleep=worker_sleep,
                nack_retry_after=nack_retry_after,
            )
        )
        worker_tasks.append(task)
        return task

    try:
        yield Client(channel), make_worker
    finally:
        for task in worker_tasks:
            task.cancel()
        for task in worker_tasks:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
        await engine.stop()
        await server.stop(grace=None)
        await channel.close()
        await conn.close()


@asynccontextmanager
async def _cluster(
    workflows: Mapping[str, WorkflowFn],
    activities: ActivityRegistry,
    *,
    db_path: str,
    worker_sleep: AsyncSleeper = asyncio.sleep,
    poll_timeout: float = 0.2,
    result_timeout: float = 5.0,
    lease_seconds: float = 30.0,
    now: Clock = time.monotonic,
) -> AsyncIterator[Client]:
    """Stand up Engine + one Worker + Client on one loop; yield the Client.

    A convenience over :func:`_server` for tests that want a single worker for the
    whole run. ``db_path`` should be a pytest ``tmp_path`` so SQLite's files are
    cleaned up automatically.
    """
    async with _server(
        workflows,
        db_path=db_path,
        poll_timeout=poll_timeout,
        result_timeout=result_timeout,
        lease_seconds=lease_seconds,
        now=now,
        worker_sleep=worker_sleep,
    ) as (client, make_worker):
        await make_worker(activities)
        yield client


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


async def test_lost_worker_redelivered_to_another(tmp_path) -> None:
    # The core 3c property: a worker that claims a task and then dies (never
    # reports) does not wedge the workflow. Its lease expires, the task is
    # redelivered, and a second worker runs it to completion -- at-least-once
    # delivery across a lost worker.
    started = asyncio.Event()  # set once a worker has claimed and entered the activity
    proceed = asyncio.Event()  # the activity blocks on this until the test allows it
    runs = 0

    async def sticky() -> str:
        nonlocal runs
        runs += 1
        started.set()
        await proceed.wait()
        return "recovered"

    async def sticky_workflow(ctx) -> str:
        return await ctx.activity("sticky")

    clock = _MonotonicFake()
    async with _server(
        {"sticky": sticky_workflow},
        db_path=str(tmp_path / "q.db"),
        lease_seconds=10.0,
        now=clock,
    ) as (client, make_worker):
        worker1 = await make_worker({"sticky": sticky})
        await client.start_workflow("wf", "sticky")
        await started.wait()  # worker1 has claimed the task and is stuck in it

        worker1.cancel()  # worker1 dies mid-activity -- it never reports
        with contextlib.suppress(asyncio.CancelledError):
            await worker1

        clock.advance(20.0)  # past the lease -> the task reappears for another
        proceed.set()  # let the redelivered run complete
        await make_worker({"sticky": sticky})  # a fresh worker picks it up

        result = await client.get_result("wf", timeout=5.0)

    assert result.status is WorkflowStatus.COMPLETED
    assert result.result == "recovered"
    # Ran once on the lost worker and once on the redelivering one -- the
    # activity executed twice (at-least-once), which is exactly what idempotency
    # keys exist to make safe.
    assert runs == 2


async def test_missing_activity_redelivered_to_a_worker_that_has_it(tmp_path) -> None:
    # A worker that lacks an activity (a rolling deploy not yet rolled here) nacks
    # the task rather than running it or dying; another worker that has it picks
    # up the redelivery and completes.
    ran = 0

    async def deployed(arg: str) -> str:
        nonlocal ran
        ran += 1
        return f"ran:{arg}"

    async def unrelated() -> str:
        return "x"

    async def deploy_workflow(ctx, arg: str) -> str:
        return await ctx.activity("deployed", arg)

    async with _server(
        {"deploy": deploy_workflow}, db_path=str(tmp_path / "q.db")
    ) as (client, make_worker):
        # worker1 is missing "deployed" (the new code has not rolled here yet).
        await make_worker({"unrelated": unrelated})
        await client.start_workflow("wf", "deploy", "payload")
        # Let worker1 claim + nack at least once before the capable worker joins,
        # so the nack path is actually exercised (not just worker2 winning first).
        await asyncio.sleep(0.05)
        await make_worker({"deployed": deployed})  # the rolled worker, has "deployed"
        result = await client.get_result("wf", timeout=5.0)

    assert result.status is WorkflowStatus.COMPLETED
    assert result.result == "ran:payload"
    assert ran == 1  # ran exactly once, on the worker that had the activity


async def test_running_returned_while_workflow_is_in_flight(tmp_path) -> None:
    # GetWorkflowResult long-polls: while the workflow is blocked, a short client
    # budget observes RUNNING; once it finishes, COMPLETED.
    proceed = asyncio.Event()

    async def slow() -> str:
        await proceed.wait()
        return "done"

    async def slow_workflow(ctx) -> str:
        return await ctx.activity("slow")

    async with _cluster(
        {"slow": slow_workflow},
        {"slow": slow},
        db_path=str(tmp_path / "q.db"),
        result_timeout=0.1,  # short server long-poll window -> RUNNING returns fast
    ) as client:
        await client.start_workflow("wf", "slow")
        running = await client.get_result("wf", timeout=0.5)
        assert running.status is WorkflowStatus.RUNNING

        proceed.set()
        done = await client.get_result("wf", timeout=5.0)

    assert done.status is WorkflowStatus.COMPLETED
    assert done.result == "done"
