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
    from utils.data import load_benchmark_returns
    from utils.clients import get_bear_lake_client

    return alt, dt, get_bear_lake_client, gt, load_benchmark_returns, mo, pl


@app.cell
def _(pl):
    results = (
        pl.read_parquet('data/portfolios/*.parquet', include_file_paths="file_path")
        .with_columns(
            pl.col('file_path').str.extract(r"portfolios/([^/]+)\.parquet").alias('portfolio')
        )
    )
    return (results,)


@app.cell
def _(dt, mo, results):
    default_start = dt.date(2023, 1, 1)
    default_end = dt.date.today()

    start_date = mo.ui.date(start=default_start, stop=default_end, value=default_start, label="Start")
    end_date = mo.ui.date(start=default_start, stop=default_end, value=default_end, label="End")

    portfolio_names = results['portfolio'].unique().sort().to_list()

    portfolios_multiselect = mo.ui.multiselect(
        options=portfolio_names, 
        label="Portfolios",
        value=portfolio_names
    )

    mo.vstack([start_date, end_date, portfolios_multiselect])
    return end_date, portfolios_multiselect, start_date


@app.cell
def _(end_date, pl, portfolios_multiselect, results, start_date):
    start = start_date.value
    end = end_date.value
    selected_portfolios = portfolios_multiselect.value

    results_agg = (
        results
        .filter(
            pl.col('date').is_between(start, end),
            pl.col('portfolio').is_in(selected_portfolios),
        )
        .group_by('date', 'portfolio')
        .agg(
            pl.col('return').mul(pl.col('weight')).sum(),
        )
        .sort('date', 'portfolio')
        .with_columns(
            pl.col('return')
            .add(1)
            .cum_prod()
            .sub(1)
            .over('portfolio')
            .alias('cumulative_return')
        )
    )
    return end, results_agg, start


@app.cell
def _(end, get_bear_lake_client, load_benchmark_returns, pl, start):
    db = get_bear_lake_client()
    benchmark_returns = (
        load_benchmark_returns(db, start, end)
        .select(
            pl.col('date'),
            pl.lit('benchmark').alias('portfolio'),
            pl.col('return'),
            pl.col('return')
            .add(1)
            .cum_prod()
            .sub(1)
            .alias('cumulative_return')
        )
    )
    return (benchmark_returns,)


@app.cell
def _(alt, benchmark_returns, pl, results_agg):
    (
        alt.Chart(pl.concat([results_agg, benchmark_returns]))
        .mark_line()
        .encode(
            x=alt.X('date', title=""),
            y=alt.Y('cumulative_return', title='Cumulative Return (%)').axis(format='%'),
            color=alt.Color('portfolio', title="Portfolio")
        )
    )
    return


@app.cell
def _(benchmark_returns, pl, results_agg):
    results_summary = (
        pl.concat([results_agg, benchmark_returns])
        .group_by('portfolio')
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
