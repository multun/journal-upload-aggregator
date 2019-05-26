from aiohttp import web

from prometheus_client import exposition, REGISTRY


def prometheus_endpoint(registry=REGISTRY):
    async def _prometheus_endpoint(request):
        accepting = request.headers["Accept"]
        print("accepting", accepting)
        encoder, content_type = exposition.choose_encoder(accepting)
        headers = (("Content-Type", content_type),)
        return web.Response(body=encoder(registry), headers=headers)

    return _prometheus_endpoint
