import datetime as dt
from utils.clients import get_bear_lake_client
from utils.backtest import run_backtest
from atium.optimizer_constraints import FullyInvested, LongOnly
from atium.objectives import MinVarianceWithTargetActiveRisk

db = get_bear_lake_client()
start = dt.date(2023, 1, 1)
end = dt.date.today()

# Run backtest
results = run_backtest(
    db=db,
    start=start,
    end=end,
    objective=MinVarianceWithTargetActiveRisk(target_active_risk=.05),
    optimizer_constraints=[FullyInvested(), LongOnly()],
)
results.write_parquet(f'data/portfolios/targetactiverisk_longonly.parquet')