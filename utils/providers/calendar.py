import datetime as dt
import bear_lake as bl
import polars as pl


class BearLakeCalendarProvider:
    def __init__(self, db: bl.Database, start: dt.date, end: dt.date) -> None:
        self.data = db.query(
            bl.table('calendar')
            .filter(pl.col('date').is_between(start, end))
            .sort('date')
        )

    def get(self, start: dt.date, end: dt.date) -> list[dt.date]:
        return self.data.filter(pl.col('date').is_between(start, end))['date'].unique().sort().to_list()
