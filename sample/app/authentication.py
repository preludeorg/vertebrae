from functools import wraps

from aiohttp import web

from vertebrae.config import Config
from vertebrae.core import strip_request, create_log

log = create_log('api')


def allowed(func):
    @wraps(func)
    async def helper(*args, **params):
        if args[1].headers.get('token') != Config.find('token'):
            log.error('[API] Unauthorized request')
            return web.Response(status=403)

        params['data'] = await strip_request(request=args[1])
        return await func(args[0], **params)
    return helper
