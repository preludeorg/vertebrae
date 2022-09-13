import aioredis

from vertebrae.config import Config


class Cache:

    def __init__(self):
        self._cache = None

    async def connect(self) -> None:
        redis = Config.find('redis')
        if redis:
            self._cache = aioredis.from_url(
                url=f'redis://{redis.get("host")}',
                port=6379,
                db=redis.get("database", 0),
                decode_responses=True
            )

    async def add(self, k, v):
        await self._cache.add(k, v)

    async def delete(self, k):
        await self._cache.delete(k)

    async def rpop(self, k):
        await self._cache.rpop(k)

    async def rpush(self, k, v):
        await self._cache.rpush(k, v)

    async def hget(self, k1, k2):
        await self._cache.hget(k1, k2)

    async def hset(self, k1, k2, v):
        await self._cache.hset(k1, k2, v)
