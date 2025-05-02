# Bloomberg Data License Client

**BDLC** is a Python-based client designed to interact with Bloomberg's API, request financial data, transform it, and store it into a specified SQL database. This client manages session handling, data retrieval, and transformation processes, making it suitable for financial data processing and analysis.

## Features

- **OAuth2-based API Authentication**: Manages secure connections with Bloomberg's API.
- **Data Request and Retrieval**: Sends data requests, listens for responses, and downloads JSON data.
- **Data Transformation**: Cleans, reformats, and adds timestamps to retrieved data.
- **Database Insertion**: Inserts transformed data into an SQL database using custom-defined schema.

## Getting Started

### Prerequisites

- Python 3.x
- Bloomberg API credentials
- Access to an SQL database

### Installation

1. Clone the repository:

    ```
    git clone https://github.com/pvotio/bbg-client.git
    cd bbg-client
    ```

2. Install dependencies:

    ```
    pip install -r requirements.txt
    ```

3. Configure environment variables by setting values in `.env` based on `.env.sample`.

### Usage

1. **Setup**: Configure API credentials and database settings in `.env`.
2. **Initialize the Client**: In `main.py`, load tickers and initialize the Bloomberg API client.
3. **Run the Client**:

    ```
    python main.py
    ```

   The client will authenticate, submit a data request, listen for data, transform it, and store it in the database.

## Overview

- **app/**: Contains the main client logic for API requests and data handling.
  - `client.py`: Manages API requests and data downloads.
  - `loader.py`: Loads ticker data from the database.
  - `session.py`: Manages OAuth2 sessions for API communication.
- **config/**: Configuration files and settings.
  - `settings.py`: Configurations for API credentials and database settings.
  - `logger.py`: Logging setup to monitor the process.
- **database/**: Database interactions.
  - `mssql.py`: Manages connections to an MSSQL database for data insertion and querying.
- **transformer/**: Data transformation operations.
  - `agent.py`: Transforms raw data into a structured format for database storage.

## Contributing

Contributions are welcome. Please open issues or submit pull requests for any enhancements or bug fixes.
