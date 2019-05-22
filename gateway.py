import struct

from aiohttp import web, ClientSession
from async_generator import async_generator, asynccontextmanager, yield_
from itertools import chain
from log import setup_logger
from watchdog import AsyncBatchWatchdog


logger = setup_logger(__name__)


async def parse_field(stream):
    line = await stream.readline()
    if not line or line == b"\n":
        return None

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


@async_generator
async def parse_stream(stream):
    while not stream.at_eof():
        await yield_(await parse_entry(stream))


class JournalGateway:
    __slots__ = ("app", "client", "target_endpoint", "watchdog")

    def __init__(self, target_endpoint):
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

    async def send_batch(self, messages):
        logger.info("messages: %s", len(messages))

    @async_generator
    async def watchdog_ctx(self, app):
        timeout = 2
        treshold = 600
        callback = self.send_batch
        async with AsyncBatchWatchdog.context(timeout, treshold, callback) as wd:
            self.watchdog = wd
            await yield_()
            self.watchdog = None

    async def upload_journal(self, request):
        count = 0
        async for log in parse_stream(request.content):
            await self.watchdog.insert(log)
            count += 1
        return web.Response(text=f"Inserted {count} log items")


if __name__ == "__main__":
    gw = JournalGateway("")
    web.run_app(gw.app)
