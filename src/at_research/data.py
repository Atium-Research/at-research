import polars as pl
from clients import get_bear_lake_client
import bear_lake as bl
import datetime as dt


def load_returns(db: bl.Database, start: dt.date, end: dt.date) -> pl.DataFrame:
    return (
        db.query(
            bl.table('stock_returns')
            .filter(
                pl.col('date').is_between(start, end)
            )
            .drop('year')
            .sort('date', 'ticker')
        )
    )

def load_idio_vol(db: bl.Database, start: dt.date, end: dt.date) -> pl.DataFrame:
    return (
        db.query(
            bl.table('idio_vol')
            .filter(
                pl.col('date').is_between(start, end)
            )
            .drop('year')
            .sort('date', 'ticker')
        )
    )

def load_universe(db: bl.Database, start: dt.date, end: dt.date) -> pl.DataFrame:
    return (
        db.query(
            bl.table('universe')
            .filter(
                pl.col('date').is_between(start, end)
            ) 
            .sort('date', 'ticker')
        )
        .drop('year')
    )