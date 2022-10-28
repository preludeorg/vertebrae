from app.routes.core_routes import CoreRoutes
from app.services.chat import ChatService
from vertebrae.config import Config
from vertebrae.core import Server, Application


if __name__ == '__main__':
    Config.load(Config.strip(env='conf/env.yml'))
    server = Server(
        applications=[
            Application(port=8079, routes=[CoreRoutes()]),
        ],
        services=[
            ChatService(name='chat')
        ])
    server.run()
