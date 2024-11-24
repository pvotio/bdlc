import json

from decouple import config

credentials_cast = lambda x: json.loads(x)  # noqa: E731
list_cast = lambda x: [  # noqa: E731
    col.strip() for col in x.split(",") if col and col != " "
]

LOG_LEVEL = config("LOG_LEVEL", default="INFO")
CREDENTIALS = config("CREDENTIALS", cast=credentials_cast)
TI_USERNUMBER = config("TI_USERNUMBER", cast=int)
TI_SERIALNUMBER = config("TI_SERIALNUMBER", cast=int)
TI_WORKSTATION = config("TI_WORKSTATION", cast=int)
IDENTIFIER_TYPE = config("IDENTIFIER_TYPE")
DB_IDS_QUERY = config("DB_IDS_QUERY")
FIELDS = config("FIELDS", cast=list_cast)
BBG_REPLY_TIMEOUT_MIN = config("BBG_REPLY_TIMEOUT_MIN", default=30, cast=int)
IGNORE_COLUMNS = config("IGNORE_COLUMNS", default=[], cast=list_cast)
OUTPUT_TABLE = config("OUTPUT_TABLE")
INSERTER_MAX_RETRIES = config("INSERTER_MAX_RETRIES", default=3, cast=int)
REQUEST_MAX_RETRIES = config("REQUEST_MAX_RETRIES", default=3, cast=int)
REQUEST_BACKOFF_FACTOR = config("REQUEST_BACKOFF_FACTOR", default=2, cast=int)
MSSQL_AD_LOGIN = config("MSSQL_AD_LOGIN", cast=bool, default=False)
MSSQL_SERVER = config("MSSQL_SERVER")
MSSQL_DATABASE = config("MSSQL_DATABASE")

if not MSSQL_AD_LOGIN:
    MSSQL_USERNAME = config("MSSQL_USERNAME")
    MSSQL_PASSWORD = config("MSSQL_PASSWORD")
