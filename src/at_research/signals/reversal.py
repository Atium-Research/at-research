import polars as pl
from clients import get_bear_lake_client
import datetime as dt
from at_research.utils.signals import compute_alphas, compute_scores
from at_research.data import load_idio_vol, load_returns, load_universe
from at_research.utils.backtest import run_backtest

db = get_bear_lake_client()
start = dt.date(2025, 12, 1)
end = dt.date(2026, 1, 31)

signal_name = 'reversal_21'

returns = load_returns(db, start, end)
universe = load_universe(db, start, end)
idio_vol = load_idio_vol(db, start, end)

signals = (
    returns
    .with_columns(
        pl.lit(signal_name).alias('name'),
        pl.col('return')
        .log1p()
        .rolling_sum(21)
        .over('ticker')
        .alias('signal')
    )
    .select('date', 'ticker', 'name', 'signal')
)

scores = compute_scores(signals)
alphas = compute_alphas(universe, scores, idio_vol)

results = run_backtest(db, start, end, alphas)
