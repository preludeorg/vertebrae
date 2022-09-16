import abc
import asyncio
import logging

from vertebrae.database import Database


class Service(abc.ABC):
    """ Each internal services is created and managed here """

    _services = dict()
    _database = Database()

    @classmethod
    def enroll(cls, name: str, impl: str) -> None:
        """ Add a new service """
        cls._services[name] = impl

    @classmethod
    def find(cls, name: str) -> ():
        """ Find a service by name """
        return cls._services.get(name)

    @classmethod
    def db(cls) -> ():
        """ Return a handler to the DB """
        return cls._database

    @classmethod
    async def initialize(cls) -> None:
        """ Connect to databases and run all service 'start' functions """
        await cls._database.connect()
        for name, service in cls._services.items():
            func = getattr(service, 'start', None)
            if callable(func):
                asyncio.create_task(func())

    @classmethod
    def create_log(cls, name: str) -> logging.Logger:
        """ Create or retrieve a logger """
        return logging.getLogger(f'vertebrae-{name}')

    @abc.abstractmethod
    def name(self) -> str:
        pass
