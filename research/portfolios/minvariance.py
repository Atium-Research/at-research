import datetime as dt
from utils.clients import get_bear_lake_client
from utils.backtest import run_backtest
from atium.optimizer_constraints import FullyInvested, LongOnly, TargetBeta
from atium.objectives import MinVariance

db = get_bear_lake_client()
start = dt.date(2023, 1, 1)
end = dt.date.today()

# Unconstrained
results = run_backtest(
    db=db,
    start=start,
    end=end,
    objective=MinVariance(),
    optimizer_constraints=[FullyInvested()],
)
results.write_parquet(f'data/portfolios/minvariance_unconstrained.parquet')

# Long only
results = run_backtest(
    db=db,
    start=start,
    end=end,
    objective=MinVariance(),
    optimizer_constraints=[FullyInvested(), LongOnly()],
)
results.write_parquet(f'data/portfolios/minvariance_longonly.parquet')

# Beta 0
results = run_backtest(
    db=db,
    start=start,
    end=end,
    objective=MinVariance(),
    optimizer_constraints=[FullyInvested(), TargetBeta(target_beta=0)],
)
results.write_parquet(f'data/portfolios/minvariance_beta0.parquet')

# Beta 1
results = run_backtest(
    db=db,
    start=start,
    end=end,
    objective=MinVariance(),
    optimizer_constraints=[FullyInvested(), TargetBeta(target_beta=1)],
)
results.write_parquet(f'data/portfolios/minvariance_beta1.parquet')

# Beta 1, long only
results = run_backtest(
    db=db,
    start=start,
    end=end,
    objective=MinVariance(),
    optimizer_constraints=[FullyInvested(), TargetBeta(target_beta=1), LongOnly()],
)
results.write_parquet(f'data/portfolios/minvariance_beta1_longonly.parquet')
