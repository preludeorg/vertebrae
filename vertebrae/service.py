import abc
import asyncio
import hashlib
import logging

from vertebrae.config import Config
from vertebrae.database import Database


class Service(abc.ABC):
    """ Each internal services is created and managed here """

    _services = dict()
    _database = Database()

    def __init__(self, name: str):
        self.log = logging.getLogger(f'vertebrae.{name}')

    @classmethod
    def enroll(cls, name: str, impl: str) -> None:
        """ Add a new service """
        cls._services[name] = impl

    @classmethod
    def find(cls, name: str) -> ():
        """ Find a service by name """
        return cls._services.get(f'vertebrae.{name}')

    @classmethod
    def db(cls, store=None) -> ():
        """ Return a handler to the DB """
        if store:
            return getattr(cls._database, store, None)
        return cls._database

    @classmethod
    async def initialize(cls) -> None:
        """ Connect to databases and run all service 'start' functions """
        tasks = []
        await cls._database.connect()
        for name, service in cls._services.items():
            func = getattr(service, 'start', None)
            if callable(func):
                tasks.append(func())
        wait = Config.find('wait_start', True)
        await asyncio.gather(*tasks) if wait else [asyncio.create_task(t) for t in tasks]

    @classmethod
    def create_log(cls, name: str) -> logging.Logger:
        """ Create or retrieve a logger """
        return logging.getLogger(f'vertebrae.{name}')

    @classmethod
    def hash(cls, s: str, algo='sha256'):
        """ Hash a string """
        func = getattr(hashlib, algo)
        return func(s.encode('utf-8')).hexdigest()
