from decouple import config

ignore_columns_cast = lambda x: list(col for col in x.strip().split(",") if col and not col == " ")

LOG_LEVEL = config("LOG_LEVEL", default="INFO")
CREDENTIALS = config("CREDENTIALS")
IS_IDENTIFIER_ISIN = config("IS_IDENTIFIER_ISIN", default=True, cast=bool)
DB_IDS_QUERY = config("DB_IDS_QUERY")
IGNORE_COLUMNS = config("IGNORE_COLUMNS", cast=ignore_columns_cast)
OUTPUT_TABLE = config("OUTPUT_TABLE")
INSERTER_MAX_RETRIES = config("INSERTER_MAX_RETRIES", default=3, cast=int)
REQUEST_MAX_RETRIES = config("REQUEST_MAX_RETRIES", default=3, cast=int)
REQUEST_BACKOFF_FACTOR = config("REQUEST_BACKOFF_FACTOR", default=2, cast=int)
MSSQL_SERVER = config("MSSQL_SERVER")
MSSQL_DATABASE = config("MSSQL_DATABASE")
MSSQL_USERNAME = config("MSSQL_USERNAME")
MSSQL_PASSWORD = config("MSSQL_PASSWORD")
