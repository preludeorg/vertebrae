import asyncio
import logging
import os
from collections import namedtuple
from json import JSONDecodeError
from logging.handlers import WatchedFileHandler

import aiohttp_cors
import aiohttp_jinja2
import jinja2
from aiohttp import web

from vertebrae.service import Service
from detect_probe.service import ProbeService


Route = namedtuple('Route', 'method route handle')
StaticRoute = namedtuple('StaticRoute', 'prefix path')


async def strip_request(request: web.Request):
    """ Strip data off request consistently regardless of method """
    if request.content_type in ['application/x-www-form-urlencoded', 'text/plain']:
        return await request.text()
    else:
        data = dict(request.match_info) | dict(request.rel_url.query)
        if request.content_type == 'application/json':
            try:
                data.update(dict(await request.json()))
            except JSONDecodeError:
                pass
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
            app.attach_routes()
            app.attach_gui()
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
        service = ProbeService(account_id=os.getenv('PRELUDE_ACCOUNT_ID'), secret=os.getenv('PRELUDE_ACCOUNT_SECRET'))
        service.start(token=service.register())

    @staticmethod
    def setup_logger(path):
        logging.basicConfig(
            level='DEBUG',
            format='%(asctime)s - %(levelname)-5s [%(name)s/%(filename)s] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
            handlers=[WatchedFileHandler(filename=path)] if path else None
        )
        for logger_name in logging.root.manager.loggerDict.keys():
            if not logger_name.startswith('vertebrae'):
                logging.getLogger(logger_name).setLevel(logging.ERROR)


class Application:
    """ An application is a API """

    def __init__(self, port, routes, client_max_size=4096, template_directory='templates'):
        self.port = port
        self.routes = routes
        self.template_directory = template_directory
        self.application = web.Application(client_max_size=client_max_size)
        self.application.router.add_route('GET', '/ping', self.pong)
        self.cors = aiohttp_cors.setup(self.application, defaults={
            "*": aiohttp_cors.ResourceOptions(
                    expose_headers="*",
                    allow_headers="*",
                )
        })

    def attach_routes(self):
        for collection in self.routes:
            for route in collection.routes():
                route_type = type(route)
                if route_type == Route:
                    self.cors.add(self.application.router.add_route(route.method, route.route, route.handle))
                elif route_type == StaticRoute:
                    self.application.router.add_static(route.prefix, route.path)

    def attach_gui(self):
        aiohttp_jinja2.setup(self.application, loader=jinja2.FileSystemLoader(f'client/{self.template_directory}'))

    async def start(self):
        runner = web.AppRunner(self.application)
        await runner.setup()
        await web.TCPSite(runner=runner, port=self.port).start()

    async def pong(self, req: web.Request) -> web.Response:
        return web.Response(status=200)
