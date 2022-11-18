import aiopg
import logging
import psycopg2

from vertebrae.config import Config


class Relational:

    def __init__(self, log):
        self.log = log
        self._pool = None

    @staticmethod
    async def __pool_execute(pool, statement, params = None, cursor_lambda = None):
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(statement, params)
                if cursor_lambda:
                    return await cursor_lambda(cur)

    async def connect(self) -> None:
        """ Establish a connection to Postgres """
        dbname = Config.find('postgres.database')
        if dbname:
            dsn = (f"user={Config.find('postgres.user')} "
                   f"password={Config.find('postgres.password')} "
                   f"host={Config.find('postgres.host')} "
                   f"port={Config.find('postgres.port')} ")
            try:
                self._pool = await aiopg.create_pool(dsn + f"dbname={dbname} ",
                                                     minsize=0, maxsize=5, timeout=10.0)
                await self.__pool_execute(self._pool, f"SELECT * FROM pg_database WHERE datname = '{dbname};'")
            except psycopg2.OperationalError:
                logging.debug(f"Database '{dbname}' does not exist")
                async with aiopg.create_pool(dsn, minsize=0, maxsize=5, timeout=10.0) as sys_conn:
                    await self.__pool_execute(sys_conn, f"CREATE DATABASE {dbname};")
                logging.debug(f"Created database '{dbname}'")
                self._pool = await aiopg.create_pool(dsn + f"dbname={dbname} ",
                                                     minsize=0, maxsize=5, timeout=10.0)
            with open('conf/schema.sql', 'r') as sql:
                await self.execute(sql.read())

    async def execute(self, statement: str, params=(), return_val=False):
        """ Run statement """
        async def cursor_operation(cur):
            if return_val:
                return (await cur.fetchone())[0]

        try:
            return await self.__pool_execute(self._pool, statement, params, cursor_operation)
        except Exception as e:
            self.log.exception(e)

    async def fetch(self, query: str, params=()):
        """ Find all matches for a query """
        async def cursor_operation(cur):
            return await cur.fetchall()

        try:
            return await self.__pool_execute(self._pool, query, params, cursor_operation)
        except Exception as e:
            self.log.exception(e)
