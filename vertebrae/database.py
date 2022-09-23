import logging

from vertebrae.stores.cache import Cache
from vertebrae.stores.directory import Directory
from vertebrae.stores.relational import Relational
from vertebrae.stores.s3 import S3


class Database:
    """ Handlers to all data stores used by this application """

    def __init__(self):
        log = logging.getLogger(f'vertebrae.database')
        self.cache = Cache(log=log)
        self.directory = Directory(log=log)
        self.relational = Relational(log=log)
        self.s3 = S3(log=log)

    async def connect(self) -> None:
        """ Establish connections to applicable databases """
        await self.relational.connect()
        await self.cache.connect()
        await self.directory.connect()
        await self.s3.connect()
