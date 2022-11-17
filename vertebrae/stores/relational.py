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
        pgdbname = Config.find('postgres_database')
        
        if pgdbname:
            """ Establish a connection to Postgres """
            dsn = ( f"user={Config.find('postgres_user')} " if Config.find('postgres_user') else ''
                    f"password={Config.find('postgres_password')} " if Config.find('postgres_password') else ''
                    f"host={Config.find('postgres_host')} " if Config.find('postgres_host') else ''
                    f"port={Config.find('postgres_port')} " if Config.find('postgres_port') else '')
            try:
                self._pool = await aiopg.create_pool(dsn + f"dbname={pgdbname} ",
                                                      minsize=0, maxsize=5, timeout=10.0)
                await self.__pool_execute(self._pool, f"SELECT * FROM pg_database WHERE datname = '{pgdbname}';")
            except psycopg2.OperationalError:
                logging.debug(f"Database '{pgdbname}' does not exist")
                async with aiopg.create_pool(dsn, minsize=0, maxsize=5, timeout=10.0) as sys_conn:
                    await self.__pool_execute(sys_conn, f"CREATE DATABASE {pgdbname};")
                logging.debug(f"Created database '{pgdbname}'")
                self._pool = await aiopg.create_pool(dsn + f"dbname={pgdbname} ",
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
