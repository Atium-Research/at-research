import datetime as dt
from utils.clients import get_bear_lake_client
from utils.data import load_returns, load_universe, load_idio_vol
from utils.signals import reversal
from atium.signals import compute_scores, compute_alphas
from utils.backtest import run_backtest

db = get_bear_lake_client()
start = dt.date(2026, 1, 1)
end = dt.date(2026, 1, 31)
lookback = 21
signal_name = f"reversal_{lookback}"

returns = load_returns(db, start, end)
universe = load_universe(db, start, end)
idio_vol = load_idio_vol(db, start, end)

signals = reversal.compute(returns, lookback)
scores = compute_scores(signals, signal_name)
alphas = compute_alphas(universe, scores, idio_vol, signal_name)

results = run_backtest(db, start, end, alphas)

print(results.summary())