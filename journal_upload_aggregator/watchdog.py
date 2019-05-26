import asyncio
import logging

from abc import ABC, abstractmethod
from async_generator import async_generator, asynccontextmanager, yield_
from collections import deque
from functools import wraps
from time import perf_counter

from .monitoring import watchdog_ticks
from .log import setup_logger


def shielded(func):
    """
    Makes so an awaitable method is always shielded from cancellation
    """

    @wraps(func)
    def wrapped(*args, **kwargs):
        return asyncio.shield(func(*args, **kwargs))

    return wrapped


logger = setup_logger(__name__)


class AsyncWatchdog(ABC):
    __slots__ = (
        "running",
        "tick_event",
        "action_done_event",
        "timeout",
        "timed_out",
        "loop_task",
        "tick_task",
        "sleep_task",
    )

    def __init__(self, timeout):
        self.running = True
        self.tick_event = asyncio.Event()
        self.action_done_event = asyncio.Event()

        self.timeout = timeout
        self.timed_out = False

        self.loop_task = None  # waits for an event
        self.tick_task = None  # ticks every X time
        self.sleep_task = None  # sleeps between ticks

    @classmethod
    @asynccontextmanager
    @async_generator
    async def context(cls, *args, **kwargs):
        watchdog = cls(*args, **kwargs)
        watchdog.start()
        await yield_(watchdog)
        await watchdog.stop()

    def signal(self):
        self.tick_event.set()

    def wait_done(self):
        return self.action_done_event.wait()

    def reset_timer(self):
        self.sleep_task.cancel()

    async def ticker(self):
        while self.running:
            self.sleep_task = asyncio.ensure_future(asyncio.sleep(self.timeout))
            try:
                await self.sleep_task
                watchdog_ticks.inc()
                self.sleep_task = None
                self.timed_out = True
                self.signal()
            except asyncio.CancelledError:
                # do nothing: cancelling the sleep task
                # resets the timer when still running, and
                # stops the ticker otherwise
                pass

    def start(self):
        self.loop_task = asyncio.ensure_future(self.loop())
        self.tick_task = asyncio.ensure_future(self.ticker())

    async def stop(self):
        assert self.tick_task is not None
        self.running = False

        # kill the ticker task
        if self.sleep_task:
            self.sleep_task.cancel()
        await self.tick_task

        # kill the action task
        self.signal()
        await self.loop_task

    async def loop(self):
        while True:
            logger.debug("watchdog loop waiting for an event")
            await self.tick_event.wait()
            self.tick_event.clear()

            timed_out = self.timed_out
            self.timed_out = False
            try:
                logger.debug("got an event, running action")
                await self.action(timed_out)
                # signal tasks waiting for the action
                self.action_done_event.set()
                self.action_done_event.clear()
            except Exception:
                logger.exception("got exception in watchdog")
            # break after the action, so that the hook is
            # able to do something when the server stops
            if not self.running:
                break

    @abstractmethod
    async def action(self, timed_out):
        pass


class AsyncBatchWatchdog(AsyncWatchdog):
    __slots__ = ("batch_size", "callback", "queue", "last_insertion", "insert_lock")

    def __init__(self, timeout, batch_size, callback):
        self.batch_size = batch_size
        self.callback = callback
        self.queue = deque()
        self.last_insertion = 0
        self.insert_lock = asyncio.Lock()
        super().__init__(timeout)

    @shielded
    async def insert(self, message):
        # ensure only one task waits for insertion to be completed
        async with self.insert_lock:
            self.queue.append(message)
            self.last_insertion = perf_counter()

            if len(self.queue) >= self.batch_size:
                self.signal()
                # don't leave the critical section until
                # insertion is completed, hence the shielding
                await self.wait_done()
                assert len(self.queue) < self.batch_size

    async def push(self):
        if not self.queue:
            return

        await self.callback(list(self.queue))
        self.queue.clear()

    async def action(self, timed_out):
        if not timed_out or perf_counter() - self.last_insertion > self.timeout:
            await self.push()
