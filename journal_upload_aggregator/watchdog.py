import asyncio
import logging

from abc import ABC, abstractmethod
from async_generator import async_generator, asynccontextmanager, yield_
from collections import deque
from functools import wraps
from time import perf_counter

from .monitoring import watchdog_ticks, concurrent_uploads
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
        logger.info("context watchdog stop")
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
        logger.info("stop routine")
        assert self.tick_task is not None
        self.running = False

        logger.info("cancelling sleep task")
        # kill the ticker task
        if self.sleep_task:
            self.sleep_task.cancel()
        await self.tick_task

        logger.info("killing loop task")
        # kill the action task
        try:
            self.signal()
            await self.loop_task
        except Exception:
            logger.exception("wtf bro")
            raise
        logger.info("done stopping")

    async def loop(self):
        try:
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
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("exception in loop")
            raise

        logger.info("watchdog loop stopping")

    @abstractmethod
    async def action(self, timed_out):
        pass


class AsyncBatchWatchdog(AsyncWatchdog):
    __slots__ = (
        "batch_size",
        "callback",
        "queue",
        "last_insertion",
        "insert_lock",
        "upload_sem",
        "push_tasks",
    )

    def __init__(self, timeout, batch_size, callback, concurrent_batches):
        self.batch_size = batch_size
        self.callback = callback
        self.queue = deque()
        self.last_insertion = 0
        self.insert_lock = asyncio.Lock()
        self.upload_sem = asyncio.Semaphore(concurrent_batches)
        self.push_tasks = []
        super().__init__(timeout)

    @shielded
    async def insert(self, message):
        # ensure only one task waits for insertion to be completed
        async with self.insert_lock:
            assert len(self.queue) < self.batch_size

            self.queue.append(message)
            self.last_insertion = perf_counter()

            if len(self.queue) >= self.batch_size:
                # don't leave the critical section until
                # insertion is completed, hence the shielding
                await self.push()

    async def push(self):
        if not self.queue:
            return

        # acquire the sem lock
        await self.upload_sem.acquire()
        concurrent_uploads.inc()

        content = list(self.queue)
        self.queue.clear()

        async def _push():
            try:
                await self.callback(content)
            finally:
                self.upload_sem.release()
                concurrent_uploads.dec()

        # asynchronously work and release the semaphore
        self.push_tasks.append(asyncio.ensure_future(_push()))

    def collect_tasks(self):
        new_push_tasks = []
        for task in self.push_tasks:
            if not task.done():
                new_push_tasks.append(task)
                continue
            try:
                task.result()
            except Exception:
                logger.exception("push task generated an exception")
                pass

        self.push_tasks = new_push_tasks

    async def action(self, timed_out):
        self.collect_tasks()

        if not timed_out or perf_counter() - self.last_insertion > self.timeout:
            await self.push()

    async def loop(self):
        logger.info("starting watchdog loop")

        try:
            await super().loop()
        finally:
            logger.info("stopping watchdog loop, waiting for tasks")

            if self.push_tasks:
                done, pending = await asyncio.wait(self.push_tasks)
                if pending:
                    logger.error("some tasks are still pending after wait: %s", pending)

                for task in done:
                    try:
                        task.result()
                    except Exception:
                        logger.exception("push task failed")

            logger.info("done waiting for push tasks")
