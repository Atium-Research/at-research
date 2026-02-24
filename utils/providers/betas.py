import datetime as dt
import bear_lake as bl
import polars as pl
from atium.types import Betas
from atium.schemas import BetasSchema


class BearLakeBetasProvider:
    def __init__(self, db: bl.Database, start: dt.date, end: dt.date) -> None:
        self.data = db.query(
            bl.table('betas')
            .rename({'predicted_beta': 'beta'})
            .filter(
                pl.col('date').is_between(start, end),
            )
            .drop('year', 'historical_beta')
            .sort('date', 'ticker')
        )

    def get(self, date_: dt.date) -> Betas:
        return BetasSchema.validate(self.data.filter(pl.col('date').eq(date_)).sort('date', 'ticker'))
