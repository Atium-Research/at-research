import polars as pl
from utils.models import Signals
from utils.schemas import SignalSchema


def compute(returns: pl.DataFrame, lookback: int = 21) -> Signals:
    return SignalSchema.validate(
        returns
        .with_columns(
            pl.lit(f'reversal_{lookback}').alias('name'),
            pl.col('return')
            .log1p()
            .rolling_sum(lookback)
            .over('ticker')
            .alias('signal')
        )
        .select('date', 'ticker', 'name', 'signal')
    )
