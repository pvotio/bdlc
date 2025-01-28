import struct
import warnings

import numpy as np
import pandas as pd
import pyodbc
from azure.identity import DefaultAzureCredential
from fast_to_sql import fast_to_sql

from config import logger, settings

warnings.filterwarnings("ignore")


def pyodbc_attrs(access_token: str) -> dict:
    SQL_COPT_SS_ACCESS_TOKEN = 1256
    token_bytes = bytes(access_token, "utf-8")
    exp_token = b""
    for i in token_bytes:
        exp_token += bytes({i}) + bytes(1)
    return {SQL_COPT_SS_ACCESS_TOKEN: struct.pack("=i", len(exp_token)) + exp_token}


class MSSQLDatabase(object):
    AD_LOGIN = settings.MSSQL_AD_LOGIN
    SERVER = settings.MSSQL_SERVER
    DATABASE = settings.MSSQL_DATABASE
    if not AD_LOGIN:
        USERNAME = settings.MSSQL_USERNAME
        PASSWORD = settings.MSSQL_PASSWORD

    def __init__(self):
        self.cnx_kwargs = {}
        if not self.AD_LOGIN:
            self.cnx_str = (
                "DRIVER={ODBC Driver 18 for SQL Server};"
                f"SERVER={self.SERVER};DATABASE={self.DATABASE};"
                f"UID={self.USERNAME};PWD={self.PASSWORD}"
            )
        else:
            token = self.fecth_token()
            self.cnx_kwargs["attrs_before"] = pyodbc_attrs(token)
            self.cnx_str = (
                "DRIVER={ODBC Driver 18 for SQL Server};"
                f"SERVER={self.SERVER};DATABASE={self.DATABASE};Encrypt=yes"
            )

    def _get_connection(self):
        return pyodbc.connect(self.cnx_str, **self.cnx_kwargs)

    def reopen_connection(self):
        try:
            if hasattr(self, "cnx") and self.cnx:
                self.cnx.close()
        except Exception as e:
            logger.warning(f"Error closing stale connection: {e}")

        self.cnx = self._get_connection()

    def select_table(self, query):
        self.reopen_connection()
        logger.info(query)
        try:
            df = pd.read_sql(query, self.cnx)
            logger.debug(f"Selected {len(df)} rows")
            return df
        except Exception as e:
            logger.error(f"Error executing SELECT query: {e}")
            raise
        finally:
            self.cnx.close()

    def insert_table(
        self, df, table_name, if_exists="append", delete_prev_records=True
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

        try:
            fast_to_sql(
                df=df,
                name=table_name,
                conn=self.cnx,
                if_exists=if_exists,
                custom=custom,
            )
            logger.info(f"Inserted {len(df)} rows into {table_name} table")
            self.cnx.commit()
        except Exception as e:
            logger.error(f"Error inserting into table {table_name}: {e}")
            raise
        finally:
            self.cnx.close()

    @staticmethod
    def fecth_token():
        credential = DefaultAzureCredential(exclude_shared_token_cache_credential=True)
        token = credential.get_token("https://database.windows.net/.default").token
        return token
