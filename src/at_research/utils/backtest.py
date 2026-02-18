from atium.data import AlphaProvider
from atium.models import Alphas
from atium.backtester import Backtester
from atium.optimizer import MVO
from atium.costs import LinearCost
from atium.objectives import MaxUtilityWithTargetActiveRisk
from atium.optimizer_constraints import LongOnly, FullyInvested
from atium.risk_model import FactorRiskModelConstructor
from atium.strategy import OptimizationStrategy
from atium.trading_constraints import MinPositionSize
from at_research.data_providers import (
    MyFactorCovariancesProvider, 
    MyBenchmarkWeightsProvider, 
    MyFactorLoadingsProvider, 
    MyIdioVolProvider, 
    MyCalendarProvider,
    MyReturnsProvider,
    CustomAlphaProvider
)
from atium.trade_generator import TradeGenerator
from atium.backtester import BacktestResult
import polars as pl
import bear_lake as bl
import datetime as dt

def run_backtest(db: bl.Database, start: dt.date, end: dt.date, alphas: pl.DataFrame) -> BacktestResult:
    strategy = OptimizationStrategy(
        alpha_provider=CustomAlphaProvider(alphas),
        benchmark_weights_provider=MyBenchmarkWeightsProvider(db, start, end),
        risk_model_constructor=FactorRiskModelConstructor(
            factor_loadings=MyFactorLoadingsProvider(db, start, end),
            factor_covariances=MyFactorCovariancesProvider(db, start, end),
            idio_vol=MyIdioVolProvider(db, start, end)
        ),
        optimizer=MVO(
            objective=MaxUtilityWithTargetActiveRisk(target_active_risk=.05),
            constraints=[LongOnly(), FullyInvested()],
        ),
    )

    backtester = Backtester()
    return backtester.run(
        start=start,
        end=end,
        rebalance_frequency='daily',
        initial_capital=100_000,
        calendar=MyCalendarProvider(db, start, end),
        returns=MyReturnsProvider(db, start, end),
        benchmark=MyBenchmarkWeightsProvider(db, start, end),
        strategy=strategy,
        cost_model=LinearCost(bps=10),
        trade_generator=TradeGenerator(
            constraints=[MinPositionSize(dollars=1)]
        )
    )