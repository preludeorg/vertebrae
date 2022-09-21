import logging

import aiopg
import psycopg2

from vertebrae.config import Config


class Relational:

    def __init__(self):
        self._pool = None

    async def connect(self) -> None:
        """ Establish a connection to Postgres """
        postgres = Config.find('postgres')
        if postgres:
            dsn = (f"user={postgres['user']} "
                   f"password={postgres['password']} "
                   f"host={postgres['host']} "
                   f"port={postgres['port']} ")
            try:
                self._pool = await aiopg.create_pool(dsn + f"dbname={postgres['database']} ",
                                                     minsize=0, maxsize=5, timeout=10.0)
                async with self._pool.acquire() as conn:
                    async with conn.cursor() as cur:
                        await cur.execute(f"SELECT FROM pg_database WHERE datname = '{postgres['database']};'")
            except psycopg2.OperationalError:
                logging.debug(f"Database '{postgres['database']}' does not exist")
                async with aiopg.create_pool(dsn, minsize=0, maxsize=5, timeout=10.0) as sys_conn:
                    async with sys_conn.acquire() as conn:
                        async with conn.cursor() as cur:
                            await cur.execute(f"CREATE DATABASE {postgres['database']};")
                logging.debug(f"Created database '{postgres['database']}'")
                self._pool = await aiopg.create_pool(dsn + f"dbname={postgres['database']} ",
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
