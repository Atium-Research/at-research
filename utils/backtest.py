from atium.backtester import Backtester
from atium.optimizer import MVO
from atium.costs import LinearCost
from atium.objectives import Objective
from atium.optimizer_constraints import OptimizerConstraint, TargetBeta
from atium.risk_model import FactorRiskModelConstructor, RiskModelConstructor
from atium.strategy import OptimizationStrategy
from atium.trading_constraints import TradingConstraint
from atium.trade_generator import TradeGenerator
from utils.providers import (
    BearLakeCalendarProvider,
    BearLakeReturnsProvider,
    BearLakeFactorLoadingsProvider,
    BearLakeFactorCovariancesProvider,
    BearLakeIdioVolProvider,
    BearLakeBenchmarkWeightsProvider,
    BearLakeAlphaProvider,
    BearLakeBetasProvider
)
from atium.types import PositionResults
import polars as pl
import bear_lake as bl
import datetime as dt


def run_backtest(
    db: bl.Database,
    start: dt.date,
    end: dt.date,
    objective: Objective,
    *,
    risk_model_constructor: RiskModelConstructor | None = None,
    alphas: pl.DataFrame | None = None,
    cost_bps: float = 10,
    initial_capital: float = 100_000,
    rebalance_frequency: str = 'daily',
    optimizer_constraints: list[OptimizerConstraint] = [],
    trading_constraints: list[TradingConstraint] = [],
) -> PositionResults:
    default_risk_model_constructor = FactorRiskModelConstructor(
        factor_loadings=BearLakeFactorLoadingsProvider(db, start, end),
        factor_covariances=BearLakeFactorCovariancesProvider(db, start, end),
        idio_vol=BearLakeIdioVolProvider(db, start, end)
    )

    has_target_beta = any(isinstance(c, TargetBeta) for c in optimizer_constraints)

    strategy = OptimizationStrategy(
        alpha_provider=BearLakeAlphaProvider.from_df(alphas) if alphas is not None else None,
        benchmark_weights_provider=BearLakeBenchmarkWeightsProvider(db, start, end),
        beta_provider=BearLakeBetasProvider(db, start, end) if has_target_beta else None,
        risk_model_constructor=risk_model_constructor if risk_model_constructor is not None else default_risk_model_constructor,
        optimizer=MVO(
            objective=objective,
            constraints=optimizer_constraints,
        ),
    )

    backtester = Backtester()
    return backtester.run(
        start=start,
        end=end,
        rebalance_frequency=rebalance_frequency,
        initial_capital=initial_capital,
        calendar_provider=BearLakeCalendarProvider(db, start, end),
        returns_provider=BearLakeReturnsProvider(db, start, end),
        strategy=strategy,
        cost_model=LinearCost(bps=cost_bps),
        trade_generator=TradeGenerator(
            constraints=trading_constraints
        )
    )
