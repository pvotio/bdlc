import urllib
import warnings

import numpy as np
import pandas as pd
import pyodbc
from decouple import config
from fast_to_sql import fast_to_sql
from sqlalchemy import create_engine

from config import logger

warnings.filterwarnings("ignore")


class MSSQLDatabase(object):
    SERVER = config("MSSQL_SERVER", cast=str)
    DATABASE = config("MSSQL_DATABASE", cast=str)
    USERNAME = config("MSSQL_USERNAME", cast=str)
    PASSWORD = config("MSSQL_PASSWORD", cast=str)

    CNX_STRING = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD}"
    )

    PARSED_CNX_URL = urllib.parse.quote_plus(CNX_STRING)

    def __init__(self):
        self.engine = create_engine(
            f"mssql+pyodbc:///?odbc_connect={self.PARSED_CNX_URL}"
        )
        self.cnx = None

    def select_table(self, query):
        self.reopen_connection()
        logger.info(query)
        df = pd.read_sql(query, self.cnx)
        logger.debug(f"Selected {len(df)} rows")
        self.cnx.close()
        return df

    def insert_table(
        self, df, table_name, if_exists="append", delete_prev_records=False
    ):
        self.reopen_connection()
        if delete_prev_records:
            try:
                query = f"DELETE FROM {table_name}"
                cursor = self.cnx.cursor()
                cursor.execute(query)
            except Exception as e:
                logger.error(f"Error on deleting {table_name} rows: {e}")

        custom = {}

        for column in df.columns.tolist():
            if "timestamp" in column.lower():
                custom[column] = "datetime"

            elif df.dtypes[column] != np.int64 and df.dtypes[column] != np.float64:
                custom[column] = "varchar(100)"

        fast_to_sql(
            df=df, name=table_name, conn=self.cnx, if_exists=if_exists, custom=custom
        )
        logger.info(f"Inserted {len(df)} rows into {table_name} table")
        self.cnx.commit()
        self.cnx.close()
        return

    def reopen_connection(self):
        if not self.cnx or not self.cnx.connected:
            self.cnx = pyodbc.connect(self.CNX_STRING)
