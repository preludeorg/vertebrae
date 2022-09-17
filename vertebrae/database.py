from vertebrae.stores.cache import Cache
from vertebrae.stores.directory import Directory
from vertebrae.stores.relational import Relational
from vertebrae.stores.s3 import S3


class Database:
    """ Handlers to all data stores used by this application """

    def __init__(self):
        self.cache = Cache()
        self.directory = Directory()
        self.relational = Relational()
        self.s3 = S3()

    async def connect(self) -> None:
        """ Establish connections to applicable databases """
        await self.relational.connect()
        await self.cache.connect()
        await self.directory.connect()
        await self.s3.connect()
