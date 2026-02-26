from utils.data import load_returns, load_factor_returns, load_universe
from utils.backtest import run_backtest
from utils.providers import BearLakeFactorLoadingsProvider, BearLakeFactorCovariancesProvider, BearLakeIdioVolProvider
from utils.clients import get_bear_lake_client
from atium.factor_model import estimate_factor_model, estimate_factor_covariances
from atium.risk_model import FactorRiskModelConstructor
from atium.objectives import MinVariance
from atium.optimizer_constraints import FullyInvested
import datetime as dt
import polars as pl

db = get_bear_lake_client()
data_start = dt.date(2000, 1, 1)
start = dt.date(2023, 1, 1)
end = dt.date.today()
factors = ['SPY', 'VLUE', 'MTUM']
window = 252
half_life = 60

returns = load_returns(db, data_start, end)
factor_returns = load_factor_returns(db, data_start, end, factors)
universe = load_universe(db, start, end)

factor_loadings, idio_vol = estimate_factor_model(
    stock_returns=returns,
    factor_returns=factor_returns,
    window=window,
    half_life=half_life
)

factor_covariances = estimate_factor_covariances(
    factor_returns=factor_returns,
    window=window,
    half_life=half_life
)

factor_loadings = universe.join(factor_loadings, on=['ticker', 'date'], how='inner')
idio_vol = universe.join(idio_vol, on=['date', 'ticker'], how='inner')

risk_model_constructor = FactorRiskModelConstructor(
    factor_loadings=BearLakeFactorLoadingsProvider.from_df(factor_loadings),
    factor_covariances=BearLakeFactorCovariancesProvider.from_df(factor_covariances),
    idio_vol=BearLakeIdioVolProvider.from_df(idio_vol)
)

results = run_backtest(
    db=db,
    start=start,
    end=end,
    objective=MinVariance(),
    optimizer_constraints=[FullyInvested()],
    risk_model_constructor=risk_model_constructor
)
results.write_parquet(f'data/risk_models/ah3_{window}_{half_life}.parquet')
