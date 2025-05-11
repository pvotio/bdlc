# Bloomberg Data License Client (BDLC)

This project provides a scalable and automated client to request, retrieve, and store financial data from the **Bloomberg Data License (DL)** REST API. It is engineered to integrate with enterprise data workflows, fetching instrument data, applying transformation logic, and persisting results into a Microsoft SQL Server database.

## Overview

### Purpose

This project automates the end-to-end process of:
- Authenticating with Bloomberg’s OAuth2-secured DL API.
- Building and submitting data requests for a universe of financial instruments.
- Listening for Bloomberg’s asynchronous delivery and downloading structured datasets.
- Transforming the data into a usable tabular format.
- Persisting it into a designated SQL Server table.

This client enables quantitative analysts, portfolio managers, and data engineers to automate ingestion of Bloomberg-licensed content into their internal databases.

## Source of Data

This project uses the **[Bloomberg Data License REST API](https://www.bloomberg.com/professional/support/api-library/)** which provides access to high-quality financial data including:

- Historical and current pricing
- Fundamental fields
- Descriptive security information
- Bloomberg-specific identifiers

Bloomberg credentials and terminal identity parameters are required for authentication and data entitlement.

## Application Flow

The main orchestration logic is encapsulated in `main.py` and includes:

1. **Initialize Client**:
   - Queries Bloomberg-entitled tickers via a SQL query using `TickerLoader`.
   - Establishes a secure OAuth2 session via `Session`.

2. **Data Request Lifecycle**:
   - A `Client` object composes and sends a data request payload.
   - The request is submitted to the Bloomberg catalog (configured as “scheduled”).
   - A listener polls for asynchronous results and downloads them once ready.

3. **Data Transformation**:
   - The `Agent` class applies transformations to the downloaded data.
   - Unwanted or redundant fields are dropped based on the configured ignore list.

4. **Database Insertion**:
   - Transformed data is inserted into the specified SQL Server table.
   - Options allow for overwriting or appending data with de-duplication logic.

## Project Structure

```
bdlc-main/
├── app/                   # Core Bloomberg DL interaction logic
│   ├── client.py          # Data request and download handling
│   ├── loader.py          # SQL ticker loader
│   └── session.py         # OAuth2 session manager
├── config/                # Logger and environment settings
├── database/              # MSSQL interface
├── transformer/           # Data transformation logic
├── main.py                # Entry point and process orchestrator
├── .env.sample            # Example environment variable template
├── Dockerfile             # Docker setup for containerized execution
```

## Environment Variables

You must provide a `.env` file based on `.env.sample`. The following environment variables are required:

| Variable | Description |
|----------|-------------|
| `LOG_LEVEL` | Logging verbosity level (`INFO`, `DEBUG`, etc.) |
| `CREDENTIALS` | Path or JSON string with Bloomberg OAuth credentials |
| `TI_USERNUMBER`, `TI_SERIALNUMBER`, `TI_WORKSTATION` | Bloomberg terminal identity values |
| `IDENTIFIER_TYPE` | Type of identifier (e.g., `ISIN`, `BBGID`, `CUSIP`) |
| `DB_IDS_QUERY` | SQL query to retrieve instrument identifiers |
| `FIELDS` | Comma-separated list of Bloomberg field mnemonics |
| `BBG_REPLY_TIMEOUT_MIN` | Timeout (in minutes) to wait for a data response |
| `IGNORE_COLUMNS` | Fields to drop during data transformation |
| `OUTPUT_TABLE` | Destination MSSQL table |
| `MSSQL_*` | SQL Server connection credentials and parameters |
| `INSERTER_MAX_RETRIES`, `REQUEST_MAX_RETRIES`, `REQUEST_BACKOFF_FACTOR` | Retry behavior settings for resiliency |

These values are consumed by the client and injected into the API request payloads and SQL connection logic.

## Docker Support

The project is fully containerized for portability.

### Build the Image
```bash
docker build -t bdlc-client .
```

### Run the Client
```bash
docker run --env-file .env bdlc-client
```

## Requirements

Install the required Python dependencies using pip:
```bash
pip install -r requirements.txt
```

The project uses `requests-oauthlib`, `SQLAlchemy`, and `pandas` for API access, SQL integration, and data manipulation.

## Running the App

Ensure that your `.env` is configured correctly and contains valid Bloomberg API credentials.

```bash
python main.py
```

Progress will be logged to the console, including request IDs, catalog lookups, and download status.

## License

This project is MIT licensed. Bloomberg API usage is subject to their licensing and compliance requirements. Ensure that your credentials and entitlement allow for the requested data.
