from loggers import logger
from settings import BASE_PATH, DATA_PATH
import polars as pl
import requests
import json
import csv
import psycopg


def get_hdb_listings(offset: int | None = None) -> dict:
    """Mock request response. In practice, this will work with pagination i.e. with the offset query param. Capped at 300"""
    path = DATA_PATH / 'hdb_listings.json'
    if offset:
        path = DATA_PATH / f'hdb_listings_offset_{offset}.json'
    with open(path, 'r') as f:
        res = json.loads(f.read())
    return res


def get_full_hdb_listings_as_polar() -> pl.DataFrame:
    """Mock the full CSV response. Returned as Polars DataFrame"""
    path = DATA_PATH / 'hdb_2017_onwards__15-04-2026.csv'
    res = pl.read_csv(path, 
                      schema_overrides={
                        'floor_area_sqm': pl.Decimal(scale=2),
                        'resale_price': pl.Decimal(scale=2)
                        }
                    )
    return res

def retry_wrapper(max_retries: int = 5, delay: int = 3, allowed_errs: tuple = (Exception, )):
    def outer(func):# -> Callable[..., Any | None]:
        def inner(*args, **kwargs):# -> Any | None:
            for i in range(1, max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except allowed_errs as err:
                    logger.error(err)
                    logger.warning(f'Retrying... {i / max_retries}')
            msg = 'Retry limit reacheed. Task aborted.'
            logger.error(msg)
        return inner
    return outer


retry_object = retry_wrapper(5, 3)
get_with_retry = retry_object(requests.get)