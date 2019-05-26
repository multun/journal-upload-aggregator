import logging
import aiohttp
import backoff
import json
import struct

from time import perf_counter

from aiohttp import web, ClientSession
from async_generator import async_generator, asynccontextmanager, yield_

from .watchdog import AsyncBatchWatchdog
from .log import setup_logger
from .prometheus import prometheus_endpoint
from .monitoring import (
    http_journal_receive_time,
    http_journal_receive_entries_count,
    journal_send_exceptions,
    journal_send_rounds,
    call_counter,
)

logger = setup_logger(__name__)


async def parse_field(stream):
    line = await stream.readline()
    if not line or line == b"\n":
        return None

    line = line[:-1]

    field_name, sep, field_value = line.partition(b"=")
    if sep:
        return (field_name, field_value)

    (field_size,) = struct.unpack("<Q", await stream.readexactly(8))
    field_value = await stream.readexactly(field_size)
    if await stream.readexactly(1) != b"\n":
        raise SyntaxError("expected \\n after binary field")
    return (field_name, field_value)


async def parse_entry(stream):
    fields = []
    while True:
        field = await parse_field(stream)
        if field is None:
            break
        fields.append(field)
    return fields


timestamps = {b"__REALTIME_TIMESTAMP", b"__MONOTONIC_TIMESTAMP"}


def format_pair(pair):
    k, v = pair
    key = k.lstrip(b"_").decode("latin1")
    if k in timestamps:
        value = int(v) // 1000
        return key, value

    value = int(v) if v.isdigit() else v.decode("latin1")
    return key, value


def format_pairs(message):
    for pair in message:
        try:
            yield format_pair(pair)
        except ValueError:
            logger.debug("couldn't format %s", pair)
            continue


def message_format(message, index):
    message_dict = dict(format_pairs(message))

    message_id = message_dict.pop("CURSOR", None)
    if message_id is None:
        logger.debug("ignored message because of missing CURSOR: %s", message)
        return None

    message_ser = json.dumps(message_dict)
    message_header = json.dumps({"index": {"_index": index, "_id": message_id}})
    return f"{message_header}\n{message_ser}\n"


def format_messages(messages, index_name):
    for message in messages:
        formatted = message_format(message, index_name)
        if formatted is None:
            continue
        yield formatted + "\n"


@async_generator
async def parse_stream(stream):
    while not stream.at_eof():
        await yield_(await parse_entry(stream))


class JournalGateway:
    __slots__ = (
        "app",
        "client",
        "target_endpoint",
        "watchdog",
        "timeout",
        "batch_size",
        "index_name",
    )

    def __init__(self, timeout, batch_size, target_endpoint, index_name):
        self.index_name = index_name
        self.timeout = timeout
        self.batch_size = batch_size
        self.target_endpoint = target_endpoint
        self.app = web.Application()
        self.app.add_routes(
            [
                web.post("/gateway/upload", self.upload_journal, name="upload_journal"),
                web.get("/health", prometheus_endpoint(), name="health"),
            ]
        )

        self.app.cleanup_ctx.append(self.client_session_ctx)
        self.app.cleanup_ctx.append(self.watchdog_ctx)

    @async_generator
    async def client_session_ctx(self, app):
        async with ClientSession() as client:
            self.client = client
            await yield_()
            self.client = None

    # indefinitely retry
    @call_counter(journal_send_rounds)
    @backoff.on_exception(backoff.expo, aiohttp.ClientError)
    @journal_send_exceptions.count_exceptions()
    async def send_batch(self, messages):
        payload = "".join(
            message_format(msg, self.index_name) + "\n" for msg in messages
        )
        logger.info("sending payload of size: %d", len(payload))
        headers = {"Content-Type": "application/x-ndjson"}
        try:
            async with self.client.post(
                self.target_endpoint,
                data=payload.encode("utf-8"),
                headers=headers,
                raise_for_status=True,
            ) as response:
                # always read the answer, otherwise the stream
                # gets in a weird state
                answer = await response.read()
                logger.debug("ES answer (code %d): %s", response.status, answer)
        except aiohttp.ClientError:
            logger.exception("posting to ES failed")
            raise

    @async_generator
    async def watchdog_ctx(self, app):
        async with AsyncBatchWatchdog.context(
            self.timeout, self.batch_size, self.send_batch
        ) as wd:
            self.watchdog = wd
            await yield_()
            self.watchdog = None

    async def upload_journal(self, request):
        upload_start = perf_counter()
        count = 0

        async for log in parse_stream(request.content):
            await self.watchdog.insert(log)
            count += 1

        delta = perf_counter() - upload_start
        http_journal_receive_time.observe(delta)
        http_journal_receive_entries_count.observe(count)
        return web.Response(text=f"Inserted {count} log items")
