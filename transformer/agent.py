import datetime

import pandas as pd


class Agent:

    def __init__(self, df, ignore_columns) -> None:
        self.df = df
        self.ignore_columns = ignore_columns

    def transform(self):
        self.reformat_date_columns()
        self.add_timestamp()
        self.remove_columns()
        return self.df

    def remove_columns(self):
        self.df.drop(columns=self.ignore_columns, inplace=True)

    def reformat_date_columns(self):
        self.df["LAST_UPDATE"] = self.df["LAST_UPDATE"].apply(
            lambda x: str(int(x)) if pd.notna(x) else None
        )
        self.df["LAST_UPDATE"] = self.df.apply(self._reformat_last_update, axis=1)
        self.df["LAST_TRADE"] = f"{self.df['LAST_TRADE_DATE']} {self.df['LAST_TRADE_TIME']}"  # noqa: E501
        self.df["timestamp_read_utc"] = self.df.apply(self.to_date, axis=1)
        del self.df["LAST_TRADE"]

    def add_timestamp(self):
        self.df["timestamp_created_utc"] = datetime.datetime.utcnow()

    @staticmethod
    def _parse_date(date_str, formats):
        for fmt in formats:
            try:
                return datetime.datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        return None

    @staticmethod
    def to_date(row):
        x, y = row["LAST_UPDATE"], row["LAST_TRADE"]
        date_str = x if x is not None else y if y is not None else None
        if date_str is None:
            return None

        date_str = str(date_str).replace("T", " ")
        date = Agent._parse_date(
            date_str, ["%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y%m%d"]
        )
        return date.strftime("%Y-%m-%d %H:%M:%S") if date else None

    @staticmethod
    def _reformat_last_update(row):
        x, y = row["LAST_UPDATE"], row["LAST_UPDATE_DT"]
        if not isinstance(x, str) and not isinstance(y, str):
            return None

        if ":" not in x:
            return Agent._parse_date(x, ["%Y%m%d"])
        else:
            date = Agent._parse_date(x, ["%Y-%m-%d %H:%M:%S"])
            if not date:
                date = Agent._parse_date(f"{y} {x}", ["%Y-%m-%d %H:%M:%S"])

        return date.strftime("%Y-%m-%d %H:%M:%S") if date else None
