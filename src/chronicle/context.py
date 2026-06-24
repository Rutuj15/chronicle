"""The workflow-facing API: ``WorkflowContext`` and its awaitable bridge.

Workflow code never touches the runtime directly. It only ever does two things
in Week 1 -- run an activity, or read the clock -- and both go through
``WorkflowContext``. Each call returns an *awaitable* whose ``await`` yields a
Command out to the runtime's driver loop and resolves to the recorded result
(CLAUDE.md §2, §5).
"""

from collections.abc import Generator

from .events import ActivityCommand, Command, JsonValue, NowCommand, SleepCommand


class _CommandAwaitable:
    """The bridge object that ``await ctx.<op>(...)`` operates on.

    Its ``__await__`` is a generator that yields the Command (handing it to the
    driver) and returns whatever the driver sends back as the result. This is
    exactly how ``asyncio.Future`` talks to an event loop -- we are just being
    our own tiny loop here, with no asyncio involved (CLAUDE.md §5).

    The Command type is the only thing that varies between activities and
    ``now()``; the await / yield / result mechanics are identical, so one bridge
    class serves every command.
    """

    def __init__(self, command: Command) -> None:
        self._command: Command = command

    def __await__(self) -> Generator[Command, JsonValue, JsonValue]:
        # This `yield` does NOT yield to a caller of this method -- it yields
        # straight out to whoever is calling coro.send() on the workflow,
        # passing through every async frame in between. That is the trick: the
        # Command reaches our driver, not some inner coroutine frame.
        result: JsonValue = yield self._command
        return result


class WorkflowContext:
    """The only object workflow code may touch.

    Each method returns a *bare awaitable* (an instance of ``_CommandAwaitable``),
    NOT a coroutine. Awaiting it is what yields the Command to the runtime and
    produces the recorded result.

    Why these are plain methods and NOT ``async def`` (the load-bearing detail):
        ``async def`` returns a coroutine object; ``await``-ing it would *run*
        that coroutine and unwrap one layer, so the Command would be yielded
        into that inner frame and never reach our driver's ``coro.send()``. By
        returning a plain awaitable whose ``__await__`` performs the yield, the
        Command propagates straight out through every async frame to our loop.
        This is the single most common bug when hand-driving coroutines
        (CLAUDE.md §5).
    """

    def activity(self, name: str, *args: JsonValue) -> _CommandAwaitable:
        """Yield intent to run a registered activity by name with positional args.

        Week 1 supports positional args only -- enough to stay JSON-serializable
        and to keep the command shape simple. Keyword args would map to a dict
        and can be added later without changing the model.
        """
        return _CommandAwaitable(ActivityCommand(name, args))

    def now(self) -> _CommandAwaitable:
        """Yield intent to read the wall clock.

        Even reading the time is non-deterministic, so it is intercepted here as
        a ``NowCommand`` and recorded, then fed back from history on replay.
        The runtime records wall-clock time as a Unix-epoch float (a JSON
        scalar), never a ``datetime``.
        """
        return _CommandAwaitable(NowCommand())

    def sleep(self, duration: float) -> _CommandAwaitable:
        """Yield intent to suspend the workflow for ``duration`` seconds.

        This is a *durable* sleep, not a busy wait: the runtime records the
        command with its absolute deadline and the workflow suspends until that
        deadline passes. A crash mid-sleep is harmless -- the recorded deadline
        survives, so on resume the workflow waits only the remainder
        (CLAUDE.md §4, Week 3).

        ``duration`` is the deterministic intent the determinism guard replays;
        the deadline the runtime derives from it is recorded only, never
        compared. The awaitable resolves to that scheduled deadline (a
        Unix-epoch float), so a workflow can observe when its timer was due
        without re-reading the clock.
        """
        return _CommandAwaitable(SleepCommand(duration))


__all__ = ["WorkflowContext"]
