import marimo

__generated_with = "0.20.2"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    import altair as alt
    import great_tables as gt
    import datetime as dt

    return alt, dt, gt, mo, pl


@app.cell
def _(pl):
    # results = pl.read_parquet('data/alphas/momentum_*.parquet', include_file_paths="file_path")
    results = (
        pl.read_parquet('data/alphas/*.parquet', include_file_paths="file_path")
        .with_columns(
            pl.col('file_path').str.extract(r"alphas/([^/]+)\.parquet").alias('signal')
        )
    )
    return (results,)


@app.cell
def _(dt, mo, results):
    default_start = dt.date(2026, 1, 3)
    default_end = dt.date.today()

    start_date = mo.ui.date(start=default_start, stop=default_end, value=default_start, label="Start")
    end_date = mo.ui.date(start=default_start, stop=default_end, value=default_end, label="End")

    signal_names = results['signal'].unique().sort().to_list()

    signals_multiselect = mo.ui.multiselect(
        options=signal_names, 
        label="Signals",
        value=signal_names
    )

    mo.vstack([start_date, end_date, signals_multiselect])
    return end_date, signals_multiselect, start_date


@app.cell
def _(end_date, pl, results, signals_multiselect, start_date):
    start = start_date.value
    end = end_date.value
    selected_signals = signals_multiselect.value

    results_agg = (
        results
        .filter(
            pl.col('date').is_between(start, end),
            pl.col('signal').is_in(selected_signals),
        )
        .group_by('date', 'signal')
        .agg(
            pl.col('weight').sum(),
            pl.col('value').sum(),
            pl.col('return').mul(pl.col('weight')).sum(),
            pl.col('pnl').sum(),
        )
        .sort('date', 'signal')
        .with_columns(
            pl.col('return')
            .add(1)
            .cum_prod()
            .sub(1)
            .over('signal')
            .alias('cumulative_return')
        )
    )

    results_agg
    return (results_agg,)


@app.cell
def _(alt, results_agg):
    (
        alt.Chart(results_agg)
        .mark_line()
        .encode(
            x=alt.X('date', title=""),
            y=alt.Y('cumulative_return', title='Cumulative Return (%)').axis(format='%'),
            color=alt.Color('signal', title="Signal")
        )
    )
    return


@app.cell
def _(pl, results_agg):
    results_summary = (
        results_agg
        .group_by('signal')
        .agg(
            pl.col('cumulative_return').last().alias('total_return'),
            pl.col('return').mean().mul(252).alias('mean_return'),
            pl.col('return').std().mul(pl.lit(252).sqrt()).alias('volatility')
        )
        .with_columns(
            pl.col('mean_return')
            .truediv('volatility')
            .alias('sharpe')
        )
        .sort('sharpe')
    )
    return (results_summary,)


@app.cell
def _(gt, results_summary):
    (
        gt.GT(results_summary)
        .tab_header('Summary Results')
        .cols_label(
            total_return="Total Return (%)",
            mean_return="Mean Return (%)",
            volatility="Volatility (%)",
            sharpe="Sharpe"
        )
        .fmt_percent(columns=['total_return', 'mean_return', 'volatility'])
        .fmt_number(columns='sharpe')
        .opt_stylize(style=5, color='gray')
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
