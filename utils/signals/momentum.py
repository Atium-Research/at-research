import polars as pl
from atium.types import Signals
from atium.schemas import SignalsSchema


def compute(returns: pl.DataFrame, window: int, shift: int) -> Signals:
    return SignalsSchema.validate(
        returns
        .with_columns(
            pl.col('return')
            .log1p()
            .rolling_sum(window)
            .shift(shift)
            .over('ticker')
            .alias('signal'),
        )
        .select('date', 'ticker', 'signal')
    )
