import datetime as dt
from utils.clients import get_bear_lake_client
from utils.data import load_returns, load_universe, load_idio_vol
from utils.signals import momentum
from atium.signals import compute_scores, compute_alphas
from utils.backtest import run_backtest

db = get_bear_lake_client()
data_start = dt.date(2000, 1, 1)
start = dt.date(2023, 1, 1)
end = dt.date.today()
shift = 21

# Load data
returns = load_returns(db, data_start, end)
universe = load_universe(db, data_start, end)
idio_vol = load_idio_vol(db, data_start, end)

for window in [30, 60, 120, 230]:
    signal_name = f"momentum_{window}_{shift}"

    # Compute alphas
    signals = momentum.compute(returns, window, shift)
    scores = compute_scores(signals)
    alphas = compute_alphas(universe, scores, idio_vol)

    # Run backtest
    results = run_backtest(db, start, end, alphas)
    results.write_parquet(f'data/alphas/{signal_name}.parquet')