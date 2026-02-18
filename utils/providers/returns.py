import datetime as dt
import bear_lake as bl
import polars as pl
from atium.types import Returns
from atium.schemas import ReturnsSchema


class BearLakeReturnsProvider:
    def __init__(self, db: bl.Database, start: dt.date, end: dt.date) -> None:
        self.data = db.query(
            bl.table('stock_returns')
            .sort('ticker', 'date')
            .with_columns(pl.col('return').shift(-1).over('ticker'))
            .filter(
                pl.col('date').is_between(start, end),
                pl.col('return').is_not_null()
            )
            .drop('year')
            .sort('date', 'ticker')
        )

    def get(self, date_: dt.date) -> Returns:
        return ReturnsSchema.validate(self.data.filter(pl.col('date').eq(date_)).sort('date', 'ticker'))
