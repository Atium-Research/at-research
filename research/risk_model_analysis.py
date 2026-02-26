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

    return alt, dt, gt, mo, pl


@app.cell
def _(pl):
    results = (
        pl.read_parquet('data/risk_models/*.parquet', include_file_paths="file_path")
        .with_columns(
            pl.col('file_path').str.extract(r"risk_models/([^/]+)\.parquet").alias('risk_model')
        )
    )
    return (results,)


@app.cell
def _(dt, mo, results):
    default_start = dt.date(2023, 1, 1)
    default_end = dt.date.today()

    start_date = mo.ui.date(start=default_start, stop=default_end, value=default_start, label="Start")
    end_date = mo.ui.date(start=default_start, stop=default_end, value=default_end, label="End")

    risk_model_names = results['risk_model'].unique().sort().to_list()

    risk_models_multiselect = mo.ui.multiselect(
        options=risk_model_names, 
        label="Risk Models",
        value=risk_model_names
    )

    mo.vstack([start_date, end_date, risk_models_multiselect])
    return end_date, risk_models_multiselect, start_date


@app.cell
def _(end_date, pl, results, risk_models_multiselect, start_date):
    start = start_date.value
    end = end_date.value
    selected_risk_models = risk_models_multiselect.value

    results_agg = (
        results
        .filter(
            pl.col('date').is_between(start, end),
            pl.col('risk_model').is_in(selected_risk_models),
        )
        .group_by('date', 'risk_model')
        .agg(
            pl.col('return').mul(pl.col('weight')).sum(),
        )
        .sort('date', 'risk_model')
        .with_columns(
            pl.col('return')
            .add(1)
            .cum_prod()
            .sub(1)
            .over('risk_model')
            .alias('cumulative_return')
        )
    )
    return (results_agg,)


@app.cell
def _(alt, results_agg):
    (
        alt.Chart(results_agg)
        .mark_line()
        .encode(
            x=alt.X('date', title=""),
            y=alt.Y('cumulative_return', title='Cumulative Return (%)').axis(format='%'),
            color=alt.Color('risk_model', title="Risk Model")
        )
    )
    return


@app.cell
def _(pl, results_agg):
    results_summary = (
        results_agg
        .group_by('risk_model')
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
