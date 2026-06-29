# Getting started

Chronicle runs durable workflows: long-running orchestration that survives
crashes and resumes exactly where it left off. This guide takes you from install
to a running workflow in a few minutes, then shows the distributed setup.

You need Python 3.12+ and [uv](https://docs.astral.sh/uv/).

## Install

Chronicle is not on PyPI yet — install it from git:

```bash
uv add git+https://github.com/Rutuj15/chronicle   # or: pip install git+...
```

## The two things you write

Everything in Chronicle is one of two kinds of code:

- **An activity** — an `async def` that does real work (HTTP, DB, files). It may
  fail. It runs once per execution and is never replayed.
- **A workflow** — an `async def(ctx, ...)` that orchestrates activities with
  `await ctx.activity(...)`. It must be deterministic: no IO, no randomness, no
  wall-clock inside it. It *is* re-run on replay, cheaply, fed recorded results.

```python
from chronicle.core.context import WorkflowContext

async def greet(name: str) -> str:                       # an activity
    return f"Hello {name}"

async def hello(ctx: WorkflowContext, name: str) -> str:  # a workflow
    return await ctx.activity("greet", name)
```

## Run it in-process (no server)

The simplest way to run a workflow is to call the runtime directly. Hand it the
workflow, the args, an event log, and a registry of activities:

```python
import asyncio
import aiosqlite
from chronicle.core.history import SqliteEventLog
from chronicle.core.runtime import ActivityRegistry, run

async def main() -> None:
    registry: ActivityRegistry = {"greet": greet}
    conn = await aiosqlite.connect("chronicle.db")
    log = SqliteEventLog(conn, "hello", asyncio.Lock())
    await log.start()
    try:
        print(await run(hello, ("world",), log, registry))   # -> Hello world
    finally:
        await conn.close()

asyncio.run(main())
```

The `SqliteEventLog` is what makes this durable: each step is committed to disk
(one `fsync`) as it happens. Run `main()` again with the same database and
workflow id and the workflow replays from history — the `greet` activity does
*not* re-run, but you get the same result. That gap between the two runs is
durable execution in miniature. `examples/hello_world.py` does exactly this and
prints which path it took.

## Run it distributed (engine + workers + client)

For real use you split into three roles. The workflow and activity code above
does not change — only the driving moves out of process.

**Engine** — the durable brain; one process. Register your workflows, hand it a
task queue and an event-log connection, and serve it over gRPC:

```python
import aiosqlite
import grpc.aio
from chronicle.proto import chronicle_pb2_grpc as pb_grpc
from chronicle.server.engine import Engine
from chronicle.server.task_queue import SqliteTaskQueue

queue = SqliteTaskQueue(await aiosqlite.connect("tasks.db"), lease_seconds=5.0)
await queue.start()
engine = Engine({"hello": hello}, queue, await aiosqlite.connect("tasks.db.events"))
await engine.start()                       # replays in-flight workflows on restart
server = grpc.aio.server()
pb_grpc.add_ChronicleServicer_to_server(engine, server)
server.add_insecure_port("127.0.0.1:50051")
await server.start()
```

**Worker** — runs your activities. Run as many as you like:

```python
import grpc.aio
from chronicle.proto import chronicle_pb2_grpc as pb_grpc
from chronicle.worker import run_worker

stub = pb_grpc.ChronicleStub(grpc.aio.insecure_channel("127.0.0.1:50051"))
await run_worker(stub, {"greet": greet})
```

**Client** — start workflows and read results, from your application:

```python
import grpc.aio
from chronicle.client import Client

client = Client(grpc.aio.insecure_channel("127.0.0.1:50051"))
await client.start_workflow("wf-1", "hello", "world")
result = await client.get_result("wf-1", timeout=60.0)
print(result.status, result.result)   # WorkflowStatus.COMPLETED  Hello world
```

A worker that dies mid-activity has its task lease expire and the work redelivers
to another worker; an engine that dies reconstructs every workflow from its
durable log on restart. `examples/distributed_demo.py` runs all three roles as
real processes (and kills a worker mid-activity to show the redelivery);
`examples/engine_crash_demo.py` kills the engine itself.

## Where to look next

Each example demonstrates one concept:

| Example | Shows |
|---------|-------|
| `hello_world.py` | a workflow, durable replay across runs |
| `durable_restart.py` | record in one process, replay in another — zero re-execution |
| `durable_timer.py` | a durable timer that survives a killed worker |
| `idempotent_charge.py` | at-least-once delivery made exactly-once via idempotency |
| `concurrent_workflows.py` | cooperative concurrency on the async engine |
| `distributed_demo.py` | engine + workers + client as real processes; lease redelivery |
| `engine_crash_demo.py` | kill the engine, restart, every workflow recovers |

## Status and limits

Chronicle is a learning project, not a production system. Before relying on it
for real work, note: no TLS or auth on the gRPC server; a single engine process
with a SQLite backend (no clustering); worker idempotency dedup is in memory and
does not survive a worker restart; no signals/queries, heartbeating, or child
workflows yet. See the README for the full status.
