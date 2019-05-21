import struct
from aiohttp import web
from itertools import chain


class AsyncBufferStream():
    __slots__ = ("offset", "buf", "stream", "old_buf")

    def __init__(self, stream):
        self.offset = 0
        self.buf = None
        self.stream = stream

    async def eof(self):
        return (await self.peek()) is None

    async def fetch_buffer(self):
        try:
            self.buf = await self.stream.__anext__()
        except StopAsyncIteration:
            self.stream = None
            return False
        self.offset = 0
        return True

    async def iter_buffers(self):
        if not await self.ensure_buffer():
            return
        yield self.buf
        while (await self.fetch_buffer()):
            yield self.buf

    async def ensure_buffer(self):
        if self.buf is None:
            return await self.fetch_buffer()
        return True

    async def peek(self):
        if self.stream is None:
            return None
        if await self.ensure_buffer():
            return self.buf[self.offset]
        return None

    def offset_add(self, incr):
        self.offset += incr
        if self.offset == len(self.buf):
            self.old_buf = self.buf
            self.buf = None

    def trash(self):
        self.offset_add(1)

    async def pop(self):
        res = await self.peek()
        if res is None:
            return None
        self.trash()
        return res

    async def pop_until(self, *items):
        res = b""
        async for buf in self.iter_buffers():
            offset = self.offset
            # find the closest item
            min_i = len(buf)
            for item in items:
                cur_i = buf.find(item, offset)
                if cur_i != -1 and cur_i < min_i:
                    min_i = cur_i

            # append relevant memory
            res += buf[offset:min_i]

            # we didn't find anything, keep looking
            if min_i == len(buf):
                continue

            # we found something, save the offset and break away
            self.offset = min_i
            return res

        raise SyntaxError("unexpected EOF")

    async def popn(self, remaining_size):
        res = b""
        async for buf in self.iter_buffers():
            offset = self.offset
            usable_size = len(buf) - offset
            copy_size = usable_size
            if remaining_size <= usable_size:
                copy_size -= usable_size - remaining_size

            remaining_size -= copy_size

            # append relevant memory
            res += buf[offset:offset + copy_size]
            if not remaining_size:
                self.offset_add(copy_size)
                return res
        raise SyntaxError("unexpected EOF")

async def request_chunks(request):
    async for data, _ in request.content.iter_chunks():
        yield data

async def parse_binary_field(buf):
    size, = struct.unpack("<Q", await buf.popn(8))
    # if size == len(buf):
    #     # the last field doesn't have a trailing \n
    #     return buf[size:], buf

    # TODO: proper error message
    content = await buf.popn(size)
    if await buf.pop() != ord("\n"):
        import pdb; pdb.set_trace()
        raise SyntaxError("expected \\n after binary field")
    return content

async def parse_text_field(buf):
    content = await buf.pop_until(ord("\n"))
    content_terminator = await buf.pop()
    assert content_terminator == ord("\n")
    return content


async def parse_field(buf):
    field_name = await buf.pop_until(ord('\n'), ord('='))
    field_name_end = await buf.pop()
    if field_name_end == ord("="):
        content = await parse_text_field(buf)
    elif field_name_end == ord("\n"):
        content = await parse_binary_field(buf)
    else: # reached end of file
        import pdb; pdb.set_trace()
        raise SyntaxError("Reached EOF in field name")
    return (field_name, content)

async def parse_entry(buf):
    fields = []
    while True:
        if await buf.peek() == ord("\n"):
            buf.trash()
            break
        fields.append(await parse_field(buf))
    return fields

async def parse_stream(request):
    stream = AsyncBufferStream(request_chunks(request))
    # async for buf in stream.iter_buffers():
    #     print(len(buf))

    while not await stream.eof():
        print(await parse_entry(stream))

    print("done")
    return web.Response(text="Thx by")

if __name__ == "__main__":
    app = web.Application()
    app.add_routes([web.post('/lol/upload', parse_stream)])
    web.run_app(app)
