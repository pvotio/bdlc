import datetime

import pandas as pd

from config import logger, settings


class Agent:

    def __init__(self, df, ignore_columns) -> None:
        self.df = df
        self.ignore_columns = ignore_columns

    def transform(self):
        self.add_empty_columns()
        self.reformat_date_columns()
        self.add_timestamp()
        self.remove_columns()
        return self.df

    def add_empty_columns(self):
        df_columns = list(self.df.columns)
        logger.debug(f"Raw BBG dataframe columns: {', '.join(df_columns)}")
        new_columns_set = self.find_columns_intersection(df_columns)
        self.df = self.df.reindex(columns=new_columns_set)
        return

    def find_columns_intersection(self, df_columns):
        """E.G. @@A is Tagged column. @@ is used to tag columns to bypass BBG API but only included
        in the output table acting as a empty columns and placeholder for further post processing
        """

        def find_unexpected_columns_range(df_columns, not_tagged_columns):
            for i in range(len(df_columns)):
                if df_columns[i:] == not_tagged_columns:
                    return (0, i - 1)
            else:
                return False

        not_tagged_columns = [
            col for col in settings.FIELDS if not col.startswith("@@")
        ]
        i_a, i_b = find_unexpected_columns_range(df_columns, not_tagged_columns)
        return list(
            df_columns[i_a:i_b] + [col.replace("@@", "") for col in settings.FIELDS]
        )

    def remove_columns(self):
        self.df.drop(columns=self.ignore_columns, inplace=True)

    def reformat_date_columns(self):
        self.df["LAST_UPDATE"] = self.df.apply(self._reformat_last_update, axis=1)
        self.df["LAST_TRADE"] = (
            f"{self.df['LAST_TRADE_DATE']} {self.df['LAST_TRADE_TIME']}"  # noqa: E501
        )
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

        if pd.isna(x):
            x = ""
        elif isinstance(x, float):
            x = str(int(x))
        elif isinstance(x, int):
            x = str(x)

        if ":" not in x:
            return Agent._parse_date(x, ["%Y%m%d"])
        else:
            date = Agent._parse_date(x, ["%Y-%m-%d %H:%M:%S"])
            if not date:
                date = Agent._parse_date(f"{y} {x}", ["%Y-%m-%d %H:%M:%S"])

        return date.strftime("%Y-%m-%d %H:%M:%S") if date else None
