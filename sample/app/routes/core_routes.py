from aiohttp import web

from app.authentication import allowed
from vertebrae.core import Route
from vertebrae.service import Service


class CoreRoutes:

    def routes(self) -> [Route]:
        return [
            Route(method='GET', route='/chat', handle=self._get_messages),
            Route(method='POST', route='/chat', handle=self._save_message),
        ]

    @allowed
    async def _get_messages(self, data: dict) -> web.json_response:
        messages = await Service.find('chat').get_msgs()
        return web.json_response(messages)

    @allowed
    async def _save_message(self, data: dict) -> web.Response:
        await Service.find('chat').save_msg(msg=data.get('text'))
        return web.Response(status=200, text='Message saved!')
