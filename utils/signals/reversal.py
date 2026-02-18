import polars as pl
from utils.models import Signals
from utils.schemas import SignalSchema


def compute(returns: pl.DataFrame, name: str, lookback: int = 21) -> Signals:
    return SignalSchema.validate(
        returns
        .with_columns(
            pl.col('return')
            .log1p()
            .rolling_sum(lookback)
            .over('ticker')
            .alias('signal'),
            pl.lit(name).alias('name')
        )
        .select('date', 'ticker', 'name', 'signal')
    )
