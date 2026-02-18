from atium.backtester import Backtester, BacktestResult
from atium.optimizer import MVO
from atium.costs import LinearCost
from atium.objectives import MaxUtilityWithTargetActiveRisk
from atium.optimizer_constraints import LongOnly, FullyInvested
from atium.risk_model import FactorRiskModelConstructor
from atium.strategy import OptimizationStrategy
from atium.trading_constraints import MinPositionSize
from atium.trade_generator import TradeGenerator
from utils.providers import (
    BearLakeCalendarProvider,
    BearLakeReturnsProvider,
    BearLakeFactorLoadingsProvider,
    BearLakeFactorCovariancesProvider,
    BearLakeIdioVolProvider,
    BearLakeBenchmarkWeightsProvider,
    CustomAlphaProvider,
)
import polars as pl
import bear_lake as bl
import datetime as dt


def run_backtest(
    db: bl.Database,
    start: dt.date,
    end: dt.date,
    alphas: pl.DataFrame,
    *,
    target_active_risk: float = 0.05,
    cost_bps: float = 10,
    initial_capital: float = 100_000,
    rebalance_frequency: str = 'daily',
    min_position_dollars: float = 1,
) -> BacktestResult:
    strategy = OptimizationStrategy(
        alpha_provider=CustomAlphaProvider(alphas),
        benchmark_weights_provider=BearLakeBenchmarkWeightsProvider(db, start, end),
        risk_model_constructor=FactorRiskModelConstructor(
            factor_loadings=BearLakeFactorLoadingsProvider(db, start, end),
            factor_covariances=BearLakeFactorCovariancesProvider(db, start, end),
            idio_vol=BearLakeIdioVolProvider(db, start, end)
        ),
        optimizer=MVO(
            objective=MaxUtilityWithTargetActiveRisk(target_active_risk=target_active_risk),
            constraints=[LongOnly(), FullyInvested()],
        ),
    )

    backtester = Backtester()
    return backtester.run(
        start=start,
        end=end,
        rebalance_frequency=rebalance_frequency,
        initial_capital=initial_capital,
        calendar=BearLakeCalendarProvider(db, start, end),
        returns=BearLakeReturnsProvider(db, start, end),
        benchmark=BearLakeBenchmarkWeightsProvider(db, start, end),
        strategy=strategy,
        cost_model=LinearCost(bps=cost_bps),
        trade_generator=TradeGenerator(
            constraints=[MinPositionSize(dollars=min_position_dollars)]
        )
    )
