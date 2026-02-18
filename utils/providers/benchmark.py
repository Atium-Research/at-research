import datetime as dt
import bear_lake as bl
import polars as pl
from atium.types import BenchmarkWeights
from atium.schemas import BenchmarkWeightsSchema


class BearLakeBenchmarkWeightsProvider:
    def __init__(self, db: bl.Database, start: dt.date, end: dt.date) -> None:
        self.data = db.query(
            bl.table('benchmark_weights')
            .filter(pl.col('date').is_between(start, end))
            .drop('year')
            .sort('date', 'ticker')
        )

    def get(self, date_: dt.date) -> BenchmarkWeights:
        return BenchmarkWeightsSchema.validate(self.data.filter(pl.col('date').eq(date_)).sort('date', 'ticker'))
