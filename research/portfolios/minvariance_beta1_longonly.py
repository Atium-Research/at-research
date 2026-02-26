import datetime as dt
from utils.clients import get_bear_lake_client
from utils.backtest import run_backtest
from atium.optimizer_constraints import FullyInvested, TargetBeta, LongOnly
from atium.objectives import MinVariance

db = get_bear_lake_client()
start = dt.date(2023, 1, 1)
end = dt.date.today()

# Run backtest
results = run_backtest(
    db=db,
    start=start,
    end=end,
    objective=MinVariance(),
    optimizer_constraints=[FullyInvested(), TargetBeta(target_beta=1), LongOnly()],
)
results.write_parquet(f'data/portfolios/minvariance_beta1_longonly.parquet')