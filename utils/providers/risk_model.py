import datetime as dt
import bear_lake as bl
import polars as pl
from atium.models import FactorLoadings, FactorCovariances, IdioVol


class BearLakeFactorLoadingsProvider:
    def __init__(self, db: bl.Database, start: dt.date, end: dt.date) -> None:
        self.data = db.query(
            bl.table('universe')
            .join(
                other=bl.table('factor_loadings'),
                on=['date', 'ticker'],
                how='left'
            )
            .filter(
                pl.col('date').is_between(start, end),
                pl.col('loading').is_not_null()
            )
            .drop('year')
            .sort('date', 'ticker', 'factor')
        )

    def get(self, date_: dt.date) -> FactorLoadings:
        return FactorLoadings.validate(self.data.filter(pl.col('date').eq(date_)).sort('date', 'ticker', 'factor'))


class BearLakeFactorCovariancesProvider:
    def __init__(self, db: bl.Database, start: dt.date, end: dt.date) -> None:
        self.data = db.query(
            bl.table('factor_covariances')
            .filter(
                pl.col('date').is_between(start, end),
                pl.col('covariance').is_not_null()
            )
            .drop('year')
            .sort('date', 'factor_1', 'factor_2')
        )

    def get(self, date_: dt.date) -> FactorCovariances:
        return FactorCovariances.validate(self.data.filter(pl.col('date').eq(date_)).sort('date', 'factor_1', 'factor_2'))


class BearLakeIdioVolProvider:
    def __init__(self, db: bl.Database, start: dt.date, end: dt.date) -> None:
        self.data = db.query(
            bl.table('universe')
            .join(
                other=bl.table('idio_vol'),
                on=['date', 'ticker'],
                how='left'
            )
            .filter(
                pl.col('date').is_between(start, end),
                pl.col('idio_vol').is_not_null()
            )
            .drop('year')
            .sort('date', 'ticker')
        )

    def get(self, date_: dt.date) -> IdioVol:
        return IdioVol.validate(self.data.filter(pl.col('date').eq(date_)).sort('date', 'ticker'))
