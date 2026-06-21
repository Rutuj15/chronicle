"""Phase 0 spike — prove manual coroutine driving works.

An ``async def`` workflow ``await``s an activity; we drive the coroutine by hand
with ``.send()``. The await yields a Command out to us, we feed a result back in.
No event log, no replay, no asyncio — just the core mechanism.

This file is a throwaway: it exists to de-risk the one idea the whole engine
rests on.

Run it:  uv run python examples/spike_coroutine.py
"""


class Command:
    """The workflow's intent: 'please run activity X with arg Y.'

    This is the object the coroutine yields OUT to whoever is driving it.
    """

    def __init__(self, activity: str, arg: object) -> None:
        self.activity = activity
        self.arg = arg

    def __repr__(self) -> str:
        return f"Command(activity={self.activity!r}, arg={self.arg!r})"


class _ActivityAwaitable:
    """The bridge object that ``await ctx.activity(...)`` operates on.

    Its ``__await__`` is a generator that yields the Command (handing it to the
    driver) and returns whatever the driver sends back as the result.
    """

    def __init__(self, command: Command) -> None:
        self._command = command

    def __await__(self):
        # This `yield` does NOT yield to a caller of this method — it yields
        # straight out to whoever is calling coro.send() on the workflow,
        # passing through every async frame in between. That is the trick.
        result = yield self._command
        return result


class WorkflowContext:
    """The workflow-facing API. ``ctx.activity(...)`` returns an awaitable.

    Deliberately a plain method, NOT ``async def``: it just builds and returns
    the awaitable. The *awaiting* is what drives the machinery.
    """

    def activity(self, activity: str, arg: object) -> _ActivityAwaitable:
        return _ActivityAwaitable(Command(activity, arg))


async def greet_and_shout(ctx: WorkflowContext, name: str) -> str:
    """A perfectly normal-looking async workflow."""
    greeting = await ctx.activity("greet", name)
    shout = await ctx.activity("shout", greeting)
    return f"{greeting} >>> {shout}"


# --- Part B: the driver -----------------------------------------------------
# A miniature event loop. We drive the workflow coroutine by hand: each .send()
# runs it until its next `await ctx.activity(...)`, which yields a Command out
# to us. We "execute" it and feed the result back in.


def execute(command: Command) -> str:
    """Pretend to run the activity. In the real engine this does real work."""
    match command.activity:
        case "greet":
            return f"hello {command.arg}"
        case "shout":
            return str(command.arg).upper()
        case _:
            raise ValueError(f"unknown activity: {command.activity!r}")


def drive(coro):
    """Drive a workflow coroutine to completion and return its result.

    The heart of the engine in miniature: a manual .send() loop.
    """
    value_to_send = None  # the result we feed back into the coroutine
    while True:
        try:
            command = coro.send(value_to_send)  # run to the next `await`
        except StopIteration as done:
            return done.value  # the workflow `return`ed -> we're done
        # We received a Command. Execute it; its result becomes the value the
        # next .send() feeds back into the awaited activity.
        value_to_send = execute(command)


def main() -> None:
    ctx = WorkflowContext()
    coro = greet_and_shout(ctx, "world")
    result = drive(coro)
    print(f"workflow returned: {result!r}")


if __name__ == "__main__":
    main()
