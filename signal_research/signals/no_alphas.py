import datetime as dt
from utils.clients import get_bear_lake_client
from utils.data import load_universe
from utils.backtest import run_backtest
import polars as pl

db = get_bear_lake_client()
start = dt.date(2023, 1, 1)
end = dt.date.today()

# Load data
universe = load_universe(db, start, end)

alphas = (
    universe
    .with_columns(pl.lit(0.0).alias('alpha'))
)

# Run backtest
results = run_backtest(db, start, end, alphas)
results.write_parquet(f'data/alphas/no_alpha.parquet')