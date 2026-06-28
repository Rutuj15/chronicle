"""Real multi-process engine-crash recovery: kill the Engine, restart it, recover.

The complement to ``distributed_demo.py`` (which kills a *Worker*). Here we kill
the *Engine* -- the durable brain -- while workflows are in flight, restart it on
the SAME durable DB, and watch every in-flight workflow reconstruct from its
recorded history and complete. The Worker and the Client survive the restart:

    uv run python examples/engine_crash_demo.py engine --addr 127.0.0.1:50051
    uv run python examples/engine_crash_demo.py worker --addr 127.0.0.1:50051 --id worker-1
    uv run python examples/engine_crash_demo.py run --addr 127.0.0.1:50051 --wf wf-a --amount 100

...or the whole story in one command -- engine + worker + client, with the
engine killed mid-workflow and restarted:

    uv run python examples/engine_crash_demo.py demo

What the demo shows (the Week-6 crash-recovery property, end to end):

A workflow parked on an in-flight activity has NO recorded event for it -- its
state lives only in the live coroutine, which dies with the Engine. On restart,
``Engine.start()`` -> ``recover()`` replays each workflow's durable history into a
fresh live coroutine and re-issues the in-flight activity. The Worker (which never
died -- it retried its polls through the blip) reconnects and runs it; the
engine-minted idempotency key dedups the at-least-once re-execution into an
exactly-once *effect*. The Client's result long-poll rode through the restart too.
The durable log is the source of truth; the live coroutine was only ever a cache.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import os
import shutil
import socket
import sys
import tempfile
from collections.abc import Coroutine
from typing import Any

import aiosqlite
import grpc.aio

from chronicle.client import Client
from chronicle.engine import Engine
from chronicle.proto import chronicle_pb2_grpc as pb_grpc
from chronicle.runtime import ActivitySpec
from chronicle.task_queue import SqliteTaskQueue
from chronicle.worker import run_worker

# --- The workflow + activity the demo runs --------------------------------
#
# `charge` is a side-effecting activity registered IDEMPOTENT. Across the Engine
# restart the in-flight activity is re-issued (at-least-once delivery); the
# engine-minted key "{workflow_id}:{seq}" lets the activity dedup. Because the
# Worker SURVIVES the restart, its in-memory dedup cache persists -- so the
# re-execution becomes an exactly-once effect. That is the whole point of
# idempotency keys: at-least-once delivery made safe.
WORKER_ID = "worker"

# Lease longer than the activity so a healthy Worker always finishes inside it;
# only a killed Engine (whose uncommitted lease reverts on restart) triggers
# re-delivery. Keeps the demo an unambiguous crash-recovery story.
LEASE_SECONDS = 5.0
ACTIVITY_SECONDS = 1.5
# Short server windows so a parked Worker re-checks promptly and recovery latency
# stays low; the Worker/Client RPC retry cadence while the Engine is down.
POLL_TIMEOUT = 0.3
RPC_BACKOFF = 0.3

# The Worker's idempotency-key -> result cache. Process-local, so it survives the
# ENGINE restart (the Worker never dies) -- which is what makes the dedup work
# across it. (It would NOT survive a Worker restart; that needs a downstream
# store, out of scope here.)
_charges: dict[str, str] = {}


async def charge(amount: int, *, idempotency_key: str) -> str:
    """The demo activity: a side-effecting charge, deduped by idempotency key.

    Registered idempotent, so the Worker injects the engine-minted key
    ``"{workflow_id}:{seq}"``. The key is stable across the original run, every
    retry, and crash-replay-reexecution -- so a second delivery of the same
    activity (after the Engine restart) hits the cache and returns the first
    run's result without re-charging.
    """
    if idempotency_key in _charges:
        cached = _charges[idempotency_key]
        print(f"    [{WORKER_ID}] dedup   {idempotency_key} -> {cached}", flush=True)
        return cached
    print(
        f"    [{WORKER_ID}] charge start: {idempotency_key} amount={amount}"
        f" (~{ACTIVITY_SECONDS}s)",
        flush=True,
    )
    await asyncio.sleep(ACTIVITY_SECONDS)
    result = f"charged-{amount}"
    _charges[idempotency_key] = result
    print(f"    [{WORKER_ID}] charge done:  {idempotency_key} -> {result}", flush=True)
    return result


async def recover_workflow(ctx: object, amount: int) -> str:
    """Issue one idempotent activity; the Engine dispatches it to a Worker."""
    return await ctx.activity("charge", amount)  # type: ignore[attr-defined]


# --- engine role ----------------------------------------------------------


async def run_engine(addr: str, db_path: str) -> None:
    """Serve the Engine (durable log + leased task queue) on ``addr``.

    ``engine.start()`` recovers every workflow persisted by a prior Engine on the
    same ``db_path`` -- so a process restarted here reconstructs in-flight
    workflows from their durable history before it serves a single RPC.
    """
    queue_conn = await aiosqlite.connect(db_path)
    queue = SqliteTaskQueue(queue_conn, lease_seconds=LEASE_SECONDS)
    await queue.start()
    # The Engine's durable state (event log + workflow metadata) in a sibling
    # file; reopened on restart, a fresh Engine recovers from it.
    event_conn = await aiosqlite.connect(db_path + ".events")
    engine = Engine(
        {"recover": recover_workflow}, queue, event_conn, poll_timeout=POLL_TIMEOUT
    )
    await engine.start()  # recover workflows in flight when the Engine last died
    server = grpc.aio.server()
    pb_grpc.add_ChronicleServicer_to_server(engine, server)
    server.add_insecure_port(addr)
    await server.start()
    print(
        f"[engine] serving on {addr} "
        f"(lease={LEASE_SECONDS}s, poll={POLL_TIMEOUT}s, db={db_path})",
        flush=True,
    )
    await asyncio.Event().wait()  # serve until the process is killed


# --- worker role ----------------------------------------------------------


async def run_worker_role(addr: str, worker_id: str) -> None:
    """Poll the Engine forever, running `charge` under its idempotent policy.

    The Worker survives an Engine restart: every Engine RPC retries UNAVAILABLE
    with backoff (``rpc_backoff``), so a bounced Engine does not bounce the
    Worker -- it just reconnects once the Engine is back.
    """
    global WORKER_ID
    WORKER_ID = worker_id
    channel = grpc.aio.insecure_channel(addr)
    print(f"[{worker_id}] polling {addr}", flush=True)
    await run_worker(
        pb_grpc.ChronicleStub(channel),
        {"charge": ActivitySpec(fn=charge, idempotent=True)},
        rpc_backoff=RPC_BACKOFF,
    )


# --- client role ----------------------------------------------------------


async def run_client(addr: str, wf_id: str, amount: int) -> None:
    """Start a workflow and print its observed outcome over gRPC."""
    channel = grpc.aio.insecure_channel(addr)
    client = Client(channel)
    await client.start_workflow(wf_id, "recover", amount)
    print(f"[client] started {wf_id!r}", flush=True)
    result = await client.get_result(wf_id, timeout=60.0, rpc_backoff=RPC_BACKOFF)
    print(
        f"[client] {wf_id}: {result.status.value}"
        + (f" -> {result.result}" if result.result is not None else ""),
        flush=True,
    )
    await channel.close()


# --- the orchestrated demo ------------------------------------------------


def _free_port() -> int:
    """A free loopback port for the Engine (handed to the subprocesses)."""
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


async def _wait_for_engine(addr: str) -> None:
    """Block until the Engine's gRPC server accepts a connection."""
    channel = grpc.aio.insecure_channel(addr)
    try:
        await asyncio.wait_for(channel.channel_ready(), timeout=15.0)
    finally:
        await channel.close()


async def _wait_for_port_down(addr: str, *, timeout: float = 5.0) -> None:
    """Block until nothing listens on ``addr`` (the old Engine is fully gone).

    After killing the Engine its listening socket lingers briefly before the
    kernel frees it; binding the same port again can race that release. Polling
    connect-until-refused makes the restart rebind deterministic.
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    host, port_str = addr.rsplit(":", 1)
    port = int(port_str)
    while loop.time() < deadline:
        try:
            _reader, writer = await asyncio.open_connection(host, port)
        except OSError:
            return  # connection refused -> nothing listening -> port is free
        writer.close()
        await writer.wait_closed()
        await asyncio.sleep(0.05)
    raise TimeoutError(f"engine on {addr} never went down")


async def _relay_until(
    stream: asyncio.StreamReader, marker: str, *, timeout: float = 10.0
) -> bool:
    """Print each line from ``stream`` until one contains ``marker`` (then stop).

    Used to watch worker-1's output for proof it has claimed and entered an
    activity, so the kill lands while an activity is genuinely in flight.
    Returns False if the stream closed or the timeout elapsed without a match.
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        try:
            line = await asyncio.wait_for(
                stream.readline(), timeout=deadline - loop.time()
            )
        except TimeoutError:
            return False
        if not line:
            return False
        text = line.decode().rstrip()
        print(text, flush=True)
        if marker in text:
            return True
    return False


async def _drain(stream: asyncio.StreamReader) -> None:
    """Print every remaining line from ``stream`` until it closes."""
    while True:
        line = await stream.readline()
        if not line:
            return
        print(line.decode().rstrip(), flush=True)


async def _terminate(
    proc: asyncio.subprocess.Process | None, timeout: float = 3.0
) -> None:
    """Terminate a subprocess cleanly (escalate to kill if it won't stop)."""
    if proc is None or proc.returncode is not None:
        return
    proc.terminate()
    try:
        await asyncio.wait_for(proc.wait(), timeout=timeout)
    except TimeoutError:
        proc.kill()
        await proc.wait()


async def run_demo() -> None:
    """Kill the Engine mid-workflow, restart it, show every workflow recover.

    Two workflows are started; the single Worker drains them serially. We crash
    the Engine once worker-1 has entered wf-a's activity: wf-a is mid-activity
    (claimed, never reported) and wf-b is queued -- BOTH parked on the Engine,
    NEITHER with a recorded event. After restart ``recover()`` reconstructs both
    from their (empty) durable histories and re-issues both activities.
    """
    workdir = tempfile.mkdtemp(prefix="chronicle-engine-crash-")
    db_path = os.path.join(workdir, "tasks.db")
    addr = f"127.0.0.1:{_free_port()}"
    py = [sys.executable, os.path.abspath(__file__)]
    workflows = [("wf-a", 100), ("wf-b", 200)]
    wf_ids = [wf_id for wf_id, _ in workflows]

    engine: asyncio.subprocess.Process | None = await asyncio.create_subprocess_exec(
        *py, "engine", "--addr", addr, "--db", db_path
    )
    worker: asyncio.subprocess.Process | None = None
    drain: asyncio.Task[None] | None = None
    poll_tasks: list[asyncio.Task[Any]] = []
    try:
        await _wait_for_engine(addr)

        # One Worker, its stdout captured so we can prove an activity is in flight
        # before the crash. The Worker SURVIVES the Engine restart (retry/backoff),
        # so its in-memory dedup persists across it -- what makes the at-least-once
        # re-execution an exactly-once effect below.
        worker = await asyncio.create_subprocess_exec(
            *py, "worker", "--addr", addr, "--id", "worker-1",
            stdout=asyncio.subprocess.PIPE,
        )
        assert worker.stdout is not None

        channel = grpc.aio.insecure_channel(addr)
        client = Client(channel)
        for wf_id, amount in workflows:
            await client.start_workflow(wf_id, "recover", amount)
        print(
            f"[demo] started {len(workflows)} workflows ({wf_ids}); "
            f"worker-1 is draining them serially",
            flush=True,
        )

        # Observe results from BEFORE the crash: these long-polls span the Engine's
        # death+restart, riding through via retry/backoff (UNAVAILABLE).
        poll_tasks.extend(
            asyncio.create_task(
                client.get_result(w, timeout=60.0, rpc_backoff=RPC_BACKOFF)
            )
            for w in wf_ids
        )

        # Proof an activity is in flight: worker-1 has claimed wf-a and entered
        # `charge`. wf-a is mid-activity, wf-b is queued -- both parked on the
        # Engine, neither with a recorded event yet.
        if not await _relay_until(worker.stdout, "charge start"):
            raise RuntimeError("worker-1 never started an activity")
        # Keep relaying the Worker's output through the crash and recovery.
        drain = asyncio.create_task(_drain(worker.stdout))

        print("[demo] *** killing the ENGINE mid-workflow ***", flush=True)
        await _terminate(engine)
        engine = None
        await _wait_for_port_down(addr)
        print(
            "[demo] the Engine is gone. wf-a's in-flight activity was claimed but\n"
            "[demo] never reported, so NO event was recorded for it -- its state\n"
            "[demo] died with the live coroutine. wf-b's queued task is still in\n"
            "[demo] the durable task DB (its uncommitted lease reverted, so it is\n"
            "[demo] immediately visible again).",
            flush=True,
        )

        print(
            f"[demo] restarting the Engine on {addr} over the SAME durable DB...",
            flush=True,
        )
        engine = await asyncio.create_subprocess_exec(
            *py, "engine", "--addr", addr, "--db", db_path
        )
        await _wait_for_engine(addr)
        print(
            "[demo] Engine back up. recover() replayed each workflow's recorded\n"
            "[demo] history into a fresh live coroutine and re-issued the in-flight\n"
            "[demo] activities. worker-1 (which never died) reconnects and drains\n"
            "[demo] them.",
            flush=True,
        )

        results = await asyncio.gather(*poll_tasks)
        await channel.close()
        for (wf_id, _), result in zip(workflows, results, strict=True):
            print(
                f"[demo] {wf_id}: {result.status.value}"
                + (f" -> {result.result}" if result.result is not None else ""),
                flush=True,
            )
        print(
            "[demo] every in-flight workflow reconstructed from its durable log and\n"
            "[demo] completed. The activity ran at-least-once across the restart;\n"
            "[demo] the Worker's idempotency-key dedup (it survived, so the cache\n"
            "[demo] persisted) made it an exactly-once effect. The durable log was\n"
            "[demo] the source of truth; the live coroutine was only a cache.",
            flush=True,
        )
    finally:
        for task in poll_tasks:
            task.cancel()
        for task in poll_tasks:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
        if drain is not None:
            drain.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await drain
        await _terminate(worker)
        await _terminate(engine)
        shutil.rmtree(workdir, ignore_errors=True)


# --- entry point ----------------------------------------------------------


def main() -> int:
    parser = argparse.ArgumentParser(description="Chronicle engine-crash demo.")
    sub = parser.add_subparsers(dest="role", required=True)

    eng = sub.add_parser("engine", help="run the Engine gRPC server (restartable)")
    eng.add_argument("--addr", default="127.0.0.1:50051")
    eng.add_argument("--db", default="chronicle-engine-crash.db")

    wrk = sub.add_parser("worker", help="run a Worker (polls the Engine)")
    wrk.add_argument("--addr", default="127.0.0.1:50051")
    wrk.add_argument("--id", default="worker")

    run = sub.add_parser("run", help="run the Client (start one workflow)")
    run.add_argument("--addr", default="127.0.0.1:50051")
    run.add_argument("--wf", default="wf-a")
    run.add_argument("--amount", type=int, default=100)

    sub.add_parser("demo", help="run the full kill-the-engine-and-recover story")

    args = parser.parse_args()

    coro: Coroutine[Any, Any, None]
    if args.role == "engine":
        coro = run_engine(args.addr, args.db)
    elif args.role == "worker":
        coro = run_worker_role(args.addr, args.id)
    elif args.role == "run":
        coro = run_client(args.addr, args.wf, args.amount)
    else:
        coro = run_demo()

    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(coro)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
