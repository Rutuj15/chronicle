"""Real multi-process Chronicle: an Engine + Workers + Client, and a crash demo.

Run the three roles as real OS processes (each in its own `python`):

    uv run python examples/distributed_demo.py engine --addr 127.0.0.1:50051
    uv run python examples/distributed_demo.py worker --addr 127.0.0.1:50051 --id worker-1
    uv run python examples/distributed_demo.py run     --addr 127.0.0.1:50051

...or run the whole story -- engine + two workers + a client, with one worker
killed mid-activity so the other recovers the task via lease redelivery -- in a
single command:

    uv run python examples/distributed_demo.py demo

What the demo shows (the headline 3c property): a worker that grabs an activity
task and then DIES does not wedge the workflow. The task was *leased*, not
deleted; when the dead worker's lease expires the Engine re-exposes it, a second
worker takes it over and runs it, and the client observes COMPLETED. That is
at-least-once delivery across a lost worker -- the foundation Week-6 crash
recovery builds on, and exactly why the engine mints idempotency keys.
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
import grpc
import grpc.aio

from chronicle.client import Client
from chronicle.engine import Engine
from chronicle.proto import chronicle_pb2_grpc as pb_grpc
from chronicle.task_queue import SqliteTaskQueue
from chronicle.worker import run_worker

# --- The workflow + activity the demo runs --------------------------------
#
# `slow_charge` simulates a side-effecting activity that takes a little wall
# clock (so there is a window to kill the worker mid-flight). It prints which
# worker process is running it, using a process-local WORKER_ID set at startup.
WORKER_ID = "worker"

# Lease shorter than the activity is NOT what we want here: it would redeliver a
# healthy worker's still-running task. We keep the lease LONGER than the activity
# so a healthy worker always finishes inside its lease, and ONLY a killed worker
# triggers redelivery (after the full lease expires). That makes the demo an
# unambiguous crash-recovery story rather than lease-expiry noise.
LEASE_SECONDS = 2.0
ACTIVITY_SECONDS = 1.5
# A short server poll window so a parked worker re-checks for an expired lease
# promptly and the redelivery latency stays low.
POLL_TIMEOUT = 0.3


async def slow_charge(amount: int) -> str:
    """The demo activity: prints its worker, sleeps, returns a result."""
    print(
        f"    [{WORKER_ID}] activity start: charge {amount}"
        f" (~{ACTIVITY_SECONDS}s)",
        flush=True,
    )
    await asyncio.sleep(ACTIVITY_SECONDS)
    print(f"    [{WORKER_ID}] activity finished: charged {amount}", flush=True)
    return f"charged-{amount}"


async def recover_workflow(ctx: object, amount: int) -> str:
    """Issue one activity; the engine dispatches it to a worker."""
    return await ctx.activity("slow_charge", amount)  # type: ignore[attr-defined]


# --- engine role ----------------------------------------------------------


async def run_engine(addr: str, db_path: str) -> None:
    """Serve the Engine (workflow registry + leased task queue) on `addr`."""
    queue_conn = await aiosqlite.connect(db_path)
    queue = SqliteTaskQueue(queue_conn, lease_seconds=LEASE_SECONDS)
    await queue.start()
    # The engine's durable state (event log + workflow metadata) in a sibling
    # file; reopened on restart, a fresh engine recovers in-flight workflows.
    event_conn = await aiosqlite.connect(db_path + ".events")
    engine = Engine(
        {"recover": recover_workflow}, queue, event_conn, poll_timeout=POLL_TIMEOUT
    )
    await engine.start()  # recover workflows in flight when the engine last died
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
    """Poll the Engine forever, running `slow_charge` under its policy."""
    global WORKER_ID
    WORKER_ID = worker_id
    channel = grpc.aio.insecure_channel(addr)
    print(f"[{worker_id}] polling {addr}", flush=True)
    await run_worker(pb_grpc.ChronicleStub(channel), {"slow_charge": slow_charge})


# --- client role ----------------------------------------------------------


async def run_client(addr: str, workflow_id: str, amount: int) -> None:
    """Start a workflow and print its observed outcome over gRPC."""
    channel = grpc.aio.insecure_channel(addr)
    client = Client(channel)
    await client.start_workflow(workflow_id, "recover", amount)
    print(f"[client] started {workflow_id!r}", flush=True)
    result = await client.get_result(workflow_id, timeout=60.0)
    print(
        f"[client] outcome: {result.status.value}"
        + (f" -> {result.result}" if result.result is not None else ""),
        flush=True,
    )
    await channel.close()


# --- the orchestrated demo ------------------------------------------------


def _free_port() -> int:
    """A free loopback port for the engine (handed to the subprocesses)."""
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


async def _wait_for_engine(addr: str) -> None:
    """Block until the engine's gRPC server accepts a connection."""
    channel = grpc.aio.insecure_channel(addr)
    try:
        await asyncio.wait_for(channel.channel_ready(), timeout=15.0)
    finally:
        await channel.close()


async def _relay_until(
    stream: asyncio.StreamReader, marker: str, *, timeout: float = 10.0
) -> bool:
    """Print each line from ``stream`` until one contains ``marker`` (then stop).

    Used to watch worker-1's output for proof it has claimed and started the
    activity, so the kill lands on the worker that actually holds the lease.
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
    """Spawn engine + workers, kill the lease-holder mid-activity, show recovery.

    worker-1 starts as the *sole* poller, so it is guaranteed to grab the task;
    the demo waits for its "activity start" line before killing it, so the kill
    always lands on the worker holding the lease. Then worker-2 joins and takes
    over once the lease expires.
    """
    workdir = tempfile.mkdtemp(prefix="chronicle-demo-")
    db_path = os.path.join(workdir, "tasks.db")
    addr = f"127.0.0.1:{_free_port()}"
    py = [sys.executable, os.path.abspath(__file__)]

    engine = await asyncio.create_subprocess_exec(
        *py, "engine", "--addr", addr, "--db", db_path
    )
    worker1: asyncio.subprocess.Process | None = None
    worker2: asyncio.subprocess.Process | None = None
    try:
        await _wait_for_engine(addr)

        # worker-1 is the only poller, so it grabs the task. Capture its stdout so
        # we can confirm it started the activity before we kill it.
        worker1 = await asyncio.create_subprocess_exec(
            *py, "worker", "--addr", addr, "--id", "worker-1",
            stdout=asyncio.subprocess.PIPE,
        )
        assert worker1.stdout is not None

        channel = grpc.aio.insecure_channel(addr)
        client = Client(channel)
        await client.start_workflow("demo-wf", "recover", 42)
        print(
            "[demo] workflow started; worker-1 is the only poller, so it grabs "
            "the task",
            flush=True,
        )

        if not await _relay_until(worker1.stdout, "activity start"):
            raise RuntimeError("worker-1 never started the activity")
        print(
            "[demo] *** killing worker-1 mid-activity "
            "(it holds the lease; it will NOT report) ***",
            flush=True,
        )
        await _terminate(worker1)
        worker1 = None
        print(
            f"[demo] worker-1 is gone. Its lease ({LEASE_SECONDS}s) must expire "
            f"before the engine re-exposes the task for worker-2...",
            flush=True,
        )

        # worker-2 joins and redelivers the task once the lease has expired.
        worker2 = await asyncio.create_subprocess_exec(
            *py, "worker", "--addr", addr, "--id", "worker-2"
        )
        result = await client.get_result("demo-wf", timeout=30.0)
        await channel.close()
        print(
            f"[demo] recovered -> {result.status.value}"
            + (f": {result.result}" if result.result is not None else ""),
            flush=True,
        )
        print(
            "[demo] worker-2 took over the redelivered task and completed the "
            "workflow. A lost worker no longer wedges a parked workflow.",
            flush=True,
        )
    finally:
        await _terminate(worker1)
        await _terminate(worker2)
        await _terminate(engine)
        shutil.rmtree(workdir, ignore_errors=True)


# --- entry point ----------------------------------------------------------


def main() -> int:
    parser = argparse.ArgumentParser(description="Chronicle multi-process demo.")
    sub = parser.add_subparsers(dest="role", required=True)

    eng = sub.add_parser("engine", help="run the Engine gRPC server")
    eng.add_argument("--addr", default="127.0.0.1:50051")
    eng.add_argument("--db", default="chronicle-demo.db")

    wrk = sub.add_parser("worker", help="run a Worker (polls the Engine)")
    wrk.add_argument("--addr", default="127.0.0.1:50051")
    wrk.add_argument("--id", default="worker")

    run = sub.add_parser("run", help="run the Client (start a workflow)")
    run.add_argument("--addr", default="127.0.0.1:50051")
    run.add_argument("--wf-id", default="demo-wf")
    run.add_argument("--amount", type=int, default=42)

    sub.add_parser("demo", help="run the full kill-and-recover story end to end")

    args = parser.parse_args()

    coro: Coroutine[Any, Any, None]
    if args.role == "engine":
        coro = run_engine(args.addr, args.db)
    elif args.role == "worker":
        coro = run_worker_role(args.addr, args.id)
    elif args.role == "run":
        coro = run_client(args.addr, args.wf_id, args.amount)
    else:
        coro = run_demo()

    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(coro)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
