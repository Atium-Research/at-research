import datetime as dt
from utils.clients import get_bear_lake_client
from utils.data import load_universe
from atium.objectives import MaxUtilityWithTargetActiveRisk
from atium.optimizer_constraints import LongOnly, FullyInvested
from utils.backtest import run_backtest
import polars as pl

db = get_bear_lake_client()
start = dt.date(2023, 1, 1)
end = dt.date.today()

objective = MaxUtilityWithTargetActiveRisk(target_active_risk=0.05)
constraints = [LongOnly(), FullyInvested()]

# Load data
universe = load_universe(db, start, end)

alphas = (
    universe
    .with_columns(pl.lit(0.0).alias('alpha'))
)

# Run backtest
results = run_backtest(db, start, end, objective, alphas=alphas, optimizer_constraints=constraints)
results.write_parquet(f'data/alphas/no_alpha.parquet')