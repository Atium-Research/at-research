import polars as pl
import bear_lake as bl
import datetime as dt


def load_returns(db: bl.Database, start: dt.date, end: dt.date) -> pl.DataFrame:
    return db.query(
        bl.table('stock_returns')
        .filter(pl.col('date').is_between(start, end))
        .drop('year')
        .sort('date', 'ticker')
    )

def load_factor_returns(db: bl.Database, start: dt.date, end: dt.date, factors: list[str] | None = None) -> pl.DataFrame:
    if factors is None:
        return db.query(
            bl.table('etf_returns')
            .filter(pl.col('date').is_between(start, end))
            .drop('year')
            .sort('date', 'ticker')
        )
    return db.query(
            bl.table('etf_returns')
            .filter(
                pl.col('date').is_between(start, end),
                pl.col('ticker').is_in(factors)
            )
            .drop('year')
            .sort('date', 'ticker')
        )


def load_universe(db: bl.Database, start: dt.date, end: dt.date) -> pl.DataFrame:
    return db.query(
        bl.table('universe')
        .filter(pl.col('date').is_between(start, end))
        .drop('year')
        .sort('date', 'ticker')
    )


def load_idio_vol(db: bl.Database, start: dt.date, end: dt.date) -> pl.DataFrame:
    return db.query(
        bl.table('idio_vol')
        .filter(pl.col('date').is_between(start, end))
        .drop('year')
        .sort('date', 'ticker')
    )

def load_benchmark_returns(db: bl.Database, start: dt.date, end: dt.date) -> pl.DataFrame:
    return db.query(
        bl.table('benchmark_returns')
        .filter(pl.col('date').is_between(start, end))
        .sort('date')
    )
