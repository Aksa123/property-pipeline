
from pathlib import Path
from typing import Self
from time import sleep
from loggers import logger
import polars as pl
import psycopg
from psycopg.rows import tuple_row, dict_row
from settings import DB_STAGING_HOST, DB_STAGING_PORT, DB_STAGING_NAME, DB_STAGING_USER, DB_STAGING_PASSWORD
from typing import Union


class DBConnection:
    def __init__(self, host: str, port: int, dbname: str, user: str, password: str, application_name: str = None ):
        self.host = host
        self.port = port
        self.dbname = dbname
        self.user = user
        self.password = password
        self.application_name = application_name
        self.conn = psycopg.connect(host=self.host, port=self.port, dbname=self.dbname, user=self.user, password=self.password, application_name=self.application_name)

    def reconnect(self):
        self.conn.close()
        self.conn = psycopg.connect(host=self.host, port=self.port, dbname=self.dbname, user=self.user, password=self.password)

    def close(self):
        self.conn.close()
        
    def exc_wrapper(func):
        def inner(self: Self, sql: str, parameters: list = [], row_factory: Union[tuple_row, dict_row] = tuple_row):
            # Auto-commit & rollback upon failure
            try:
                res = func(self, sql, parameters, row_factory)
                self.conn.commit()
                return res
            except Exception as err:
                self.conn.rollback()
                raise err
        return inner
    
    def retry_wrapper(count: int = 5, delay: int = 2):
        def outer(func):
            def inner(self: Self, sql: str, parameters: list = [], row_factory: Union[tuple_row, dict_row] = tuple_row):
                last_err = None
                for i in range(1, count+1):
                    try:
                        res = func(self, sql, parameters, row_factory)
                        return res
                    # Only retry for connection-specific issues e.g. OperationalError
                    except (psycopg.errors.OperationalError, psycopg.errors.InternalError) as operr:
                        logger.error(f'error occurred. reconnecting database and retrying transaction... ( {i} / {count} )')
                        sleep(delay)
                        try:
                            self.reconnect()
                            logger.info('reconnected!')
                        except psycopg.errors.OperationalError:
                            pass
                        last_err = operr
                logger.error('retry attempt limit reached >_>')
                raise last_err
            return inner
        return outer
    
    def fetch_to_polars(self, sql: str, parameters: list = None):
        if parameters != None:
            opts = {'parameters': parameters}
        else:
            opts = None
        polars_df = pl.read_database(query=sql, connection=self, execute_options=opts)
        return polars_df

    @retry_wrapper(5,2)
    @exc_wrapper
    def execute(self, sql: str, parameters=[], row_factory=tuple_row):
        cur = self.conn.cursor(row_factory=row_factory)
        return cur.execute(sql, parameters)
    
    @retry_wrapper(5,2)
    @exc_wrapper
    def executemany(self, sql: str, parameters=[], row_factory=tuple_row, returning=False):
        cur = self.conn.cursor(row_factory=row_factory)
        res =  cur.executemany(sql, parameters)
        self.conn.commit()
        if returning:
            rows = []
            while True:
                rows.append(cur.fetchone()[0])
                if not cur.nextset():
                    break
            return rows
        return res
    
    def __del__(self):
        self.close()
    
    

conn_staging = DBConnection(host=DB_STAGING_HOST, port=DB_STAGING_PORT, dbname=DB_STAGING_NAME, user=DB_STAGING_USER, password=DB_STAGING_PASSWORD, application_name='dbconn_staging')

