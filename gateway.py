#!/usr/bin/python3

import aiohttp
import backoff
import json
import struct
import sys

from aiohttp import web, ClientSession
from argparse import ArgumentParser
from async_generator import async_generator, asynccontextmanager, yield_
from itertools import chain
from log import setup_logger
from watchdog import AsyncBatchWatchdog


logger = setup_logger(__name__)


DEFAULT_HOST = "localhost"
DEFAULT_PORT = 8080
DEFAULT_WATCHDOG_TIMEOUT = 2
DEFAULT_BATCH_SIZE = 500


def _argument_parser():
    parser = ArgumentParser(
        description="Start a systemd to elasticsearch journal aggregator"
    )

    parser.add_argument("--host", default=DEFAULT_HOST, help="Hostname to listen to")
    parser.add_argument(
        "--watchdog-timeout",
        default=DEFAULT_WATCHDOG_TIMEOUT,
        type=float,
        help="Interval at which a watchdog should check for old incomplete batches",
    )
    parser.add_argument(
        "--batch-size",
        default=DEFAULT_BATCH_SIZE,
        type=int,
        help="How big does a batch needs to be before being sent",
    )
    parser.add_argument(
        "-p", "--port", default=DEFAULT_PORT, type=int, help="Port to listen to"
    )
    parser.add_argument("elastic-url", help="Url to send batched logs to")
    parser.add_argument("index-name", help="Name of the elasticsearch index")
    return parser


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


def message_format(message, index):
    message_dict = {
        k.lstrip(b"_").decode("latin1"): int(v) if v.isdigit() else v.decode("latin1")
        for k, v in message
    }

    message_id = message_dict.pop("CURSOR")
    message_ser = json.dumps(message_dict)
    message_header = json.dumps({"index": {"_index": index, "_id": message_id}})
    return f"{message_header}\n{message_ser}\n"


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
                web.post("/gateway/upload", self.upload_journal, name="upload_journal")
                # TODO: port prometheus_client to aiohttp
                # https://github.com/prometheus/client_python/blob/master/prometheus_client/twisted/_exposition.py
                # web.get('/health', self.health, name='health'),
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
    @backoff.on_exception(backoff.expo, aiohttp.ClientError)
    async def send_batch(self, messages):
        if not messages:
            return

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
                pass
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
        count = 0
        async for log in parse_stream(request.content):
            await self.watchdog.insert(log)
            count += 1
        return web.Response(text=f"Inserted {count} log items")


def main(args=None):
    if args is None:
        args = sys.argv[1:]

    options = _argument_parser().parse_args(args=args)
    gw = JournalGateway(
        options.watchdog_timeout,
        options.batch_size,
        getattr(options, "elastic-url"),
        getattr(options, "index-name"),
    )
    web.run_app(gw.app, host=options.host, port=options.port)


if __name__ == "__main__":
    main()
