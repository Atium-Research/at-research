import polars as pl
from atium.types import Signals
from atium.schemas import SignalsSchema


def compute(returns: pl.DataFrame, lookback: int) -> Signals:
    return SignalsSchema.validate(
        returns
        .with_columns(
            pl.col('return')
            .log1p()
            .rolling_sum(lookback)
            .mul(-1)
            .over('ticker')
            .alias('signal'),
        )
        .select('date', 'ticker', 'signal')
    )
