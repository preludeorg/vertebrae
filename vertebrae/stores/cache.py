import aioredis

from vertebrae.config import Config


class Cache:

    def __init__(self, log):
        self.log = log
        self._cache = None

    async def connect(self) -> None:
        """ Establish a connection to Redis """
        redis = Config.find('redis')
        if redis:
            self._cache = aioredis.from_url(
                url=f'redis://{redis.get("host")}',
                port=6379,
                db=redis.get("database", 0),
                decode_responses=True
            )

    async def get(self, k):
        return await self._cache.get(k)

    async def get_del(self, k):
        return await self._cache.execute_command('GETDEL', k)

    async def set(self, k, v):
        await self._cache.set(k, v)

    async def delete(self, k):
        await self._cache.delete(k)

    async def rpop(self, k):
        return await self._cache.rpop(k)

    async def rpush(self, k, v):
        await self._cache.rpush(k, v)

    async def hget(self, k1, k2):
        return await self._cache.hget(k1, k2)

    async def hset(self, k1, k2, v):
        await self._cache.hset(k1, k2, v)
