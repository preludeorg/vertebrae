import asyncio
import logging
import os
from collections import namedtuple
from logging.handlers import WatchedFileHandler

import aiohttp_jinja2
import jinja2
from aiohttp import web

from vertebrae.service import Service
from detect_probe.service import ProbeService


Route = namedtuple('Route', 'method route handle')


async def strip_request(request: web.Request):
    """ Strip data off request consistently regardless of method """
    if request.content_type == 'application/x-www-form-urlencoded':
        return await request.text()
    else:
        data = dict(request.match_info)
        if request.method == 'GET':
            data.update(dict(request.rel_url.query))
        elif request.content_type == 'application/json':
            data.update(dict(await request.json()))
        return data


def create_log(name: str) -> logging.Logger:
    """ Create a logging object for general use """
    return logging.getLogger(f'vertebrae.{name}')


class Server:
    """ A server is the driver that runs applications """

    def __init__(self, services, applications):
        self.setup_logger(path=os.getenv('logfile'))
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

        for app in applications:
            self.loop.run_until_complete(app.start())
        for service in services:
            Service.enroll(service.log.name, service)
        create_log('server').info(f'Serving {len(applications)} apps with {len(services)} services')

    def run(self):
        try:
            self.start_probe()
            self.loop.run_until_complete(Service.initialize())
            self.loop.run_forever()
        except KeyboardInterrupt:
            logging.info('Keyboard interrupt received')

    @staticmethod
    def start_probe():
        """ Detach a Detect Probe inside the application process """
        service = ProbeService(account_id=os.getenv('PRELUDE_ACCOUNT_ID'), secret=os.getenv('PRELUDE_ACCOUNT_TOKEN'))
        service.start(token=service.register())

    @staticmethod
    def setup_logger(path):
        logging.basicConfig(
            level='DEBUG',
            format='%(asctime)s - %(levelname)-5s [%(name)s] %(message)s',
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
        try:
            self.application.router.add_static('/client', 'client', append_version=True)
            aiohttp_jinja2.setup(self.application, loader=jinja2.FileSystemLoader('client/templates'))
        except:
            create_log('server').info('No GUI attached. See pypi.org/project/vertebrae for more info.')
        runner = web.AppRunner(self.application)
        await runner.setup()
        await web.TCPSite(runner=runner, port=self.port).start()

    async def pong(self, req: web.Request) -> web.Response:
        return web.Response(status=200)
