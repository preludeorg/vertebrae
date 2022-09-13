import logging

import aiopg

from vertebrae.config import Config


class Relational:

    def __init__(self):
        self._pool = None

    async def connect(self) -> None:
        """ Establish a connection to the databases """
        postgres = Config.find('postgres')
        if postgres:
            self._pool = await aiopg.create_pool(f"dbname={postgres['database']} "
                                                 f"user={postgres['user']} "
                                                 f"password={postgres['password']} "
                                                 f"host={postgres['host']} "
                                                 f"port={postgres['port']} ",
                                                 minsize=0, maxsize=5, timeout=10.0)
            with open('conf/schema.sql', 'r') as sql:
                await self.execute(sql.read())

    async def execute(self, statement: str, params=(), return_id=False):
        """ Run statement retrieving either nothing or the row ID """
        try:
            async with self._pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(statement, params)
                    if return_id:
                        return (await cur.fetchone())[0]
        except Exception as e:
            logging.exception(e)

    async def fetch(self, query: str, params=()):
        """ Find all matches for a query """
        try:
            async with self._pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(query, params)
                    return await cur.fetchall()
        except Exception as e:
            logging.exception(e)
