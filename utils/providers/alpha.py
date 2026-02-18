import datetime as dt
import bear_lake as bl
import polars as pl
from atium.types import Alphas
from atium.schemas import AlphasSchema


class BearLakeAlphaProvider:
    def __init__(self, db: bl.Database, start: dt.date, end: dt.date) -> None:
        self.data = db.query(
            bl.table('universe')
            .join(
                other=bl.table('alphas'),
                on=['date', 'ticker'],
                how='left'
            )
            .with_columns(pl.col('alpha').fill_null(0))
            .filter(
                pl.col('date').is_between(start, end),
                pl.col('alpha').is_not_null()
            )
            .drop('year', 'signal')
            .sort('date', 'ticker')
        )

    def get(self, date_: dt.date) -> Alphas:
        return AlphasSchema.validate(self.data.filter(pl.col('date').eq(date_)).sort('date', 'ticker'))

    @classmethod
    def from_df(cls, alphas: Alphas) -> 'BearLakeAlphaProvider':
        instance = cls.__new__(cls)
        instance.data = alphas
        return instance

