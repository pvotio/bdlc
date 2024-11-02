from app import Client
from app.loader import TickerLoader
from app.session import get_session
from config import logger, settings
from database.mssql import MSSQLDatabase
from transformer import Agent


def load_tickers():
    try:
        loader = TickerLoader(settings.DB_IDS_QUERY)
        instruments = loader.fetch()[:5]
        logger.info(
            f"Loaded instruments: {instruments[:3]}...",
        )
        return instruments
    except Exception as e:
        logger.error(f"Failed to load instruments: {e}")
        raise


def init_client(instruments):
    try:
        session = get_session(settings.CREDENTIALS)
        client = Client(
            instruments=instruments,
            fields=settings.FIELDS,
            session=session,
            ti_usernumber=settings.TI_USERNUMBER,
            ti_serialnumber=settings.TI_SERIALNUMBER,
            ti_workstation=settings.TI_WORKSTATION,
            reply_timeout_min=settings.BBG_REPLY_TIMEOUT_MIN,
            identifier_type=settings.IDENTIFIER_TYPE,
        )
        logger.info("Client created successfully")
        return client
    except Exception as e:
        logger.error(f"Failed to create Client: {e}")
        raise


def main():
    logger.info("Initializing Data License Client")
    db_instance = MSSQLDatabase()
    instruments = load_tickers()
    client = init_client(instruments)
    client.data_request()
    logger.info("Data request sent to Bloomberg API")
    df = client.listen()
    if df is None:
        logger.warning("No data received from Bloomberg API")
        return
    else:
        logger.info("Data received, beginning transformation")

    logger.info("Transforming Data")
    agent = Agent(df, settings.IGNORE_COLUMNS)
    df = agent.transform()
    if df is None or df.empty:
        logger.warning("No data to transform or transformed DataFrame is empty")
        return
    else:
        logger.info("Data transformation completed")

    logger.info("Preparing Database Inserter")
    if df is None:
        logger.warning("No data to be inserted.")
        return

    logger.info(f"\n{df}")
    db_instance.insert_table(
        df, settings.OUTPUT_TABLE, if_exists="append", delete_prev_records=True
    )
    logger.info("Processing complete")


if __name__ == "__main__":
    main()
