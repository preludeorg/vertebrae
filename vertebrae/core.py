import asyncio
import logging
import os
from collections import namedtuple
from logging.handlers import WatchedFileHandler

from aiohttp import web

from vertebrae.service import Service

Route = namedtuple('Route', 'method route handle')


class Server:
    """ A server is the driver that runs applications """

    def __init__(self, services, applications):
        self.setup_logger(path=os.getenv('logfile'))
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

        for app in applications:
            self.loop.run_until_complete(app.start())
        for service in services:
            Service.enroll(service.name(), service)
        Service.create_log('server').info(f'Serving {len(applications)} apps with {len(services)} services')

    def run(self):
        try:
            self.loop.run_until_complete(Service.initialize())
            self.loop.run_forever()
        except KeyboardInterrupt:
            logging.info('Keyboard interrupt received')

    @staticmethod
    def setup_logger(path):
        logging.basicConfig(
            level='DEBUG',
            format='%(asctime)s - %(levelname)-5s (%(name)s) %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
            handlers=[WatchedFileHandler(filename=path)] if path else None
        )
        for logger_name in logging.root.manager.loggerDict.keys():
            if not logger_name.startswith('vertebrae'):
                logging.getLogger(logger_name).setLevel(logging.ERROR)


class Application:
    """ An application is a API """

    def __init__(self, port, routes):
        self.port = port
        self.application = web.Application(client_max_size=4096)

        for collection in routes:
            for route in collection.routes():
                self.application.router.add_route(route.method, route.route, route.handle)
        self.application.router.add_route('GET', '/ping', self.pong)

    async def start(self):
        runner = web.AppRunner(self.application)
        await runner.setup()
        await web.TCPSite(runner=runner, port=self.port).start()

    async def pong(self, req: web.Request) -> web.Response:
        return web.Response(status=200)
