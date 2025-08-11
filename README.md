# Equal-Weighted Top 100 US Stocks Index Generator
This project automates the process of building a daily rebalanced equal-weighted index using the Top 100 US stocks by market cap, over a rolling 1-month period. The pipeline integrates with Polygon.io for financial data and stores it in a SQLite database. Final metrics and analytics are exported to a structured Excel report.

## Key Features
Fetch close price and market cap data from Polygon.io API.

Identify top 100 stocks by market cap daily.

Construct a daily-rebalanced equal-weighted index.

Export composition, returns, and performance summaries to Excel.

Modular and scalable codebase with SQLite as a backend.

## Project Structure
``├── config/
│   └── config.ini                # Stores API keys and database paths
├── data/
│   └── constituents.csv          # (Optional) Static ticker source like S&P 500
├── reports/
│   └── equal_weighted_index_data.xlsx  # Final Excel report (auto-generated)
├── SQLite_DBs/
│   └── stock_data.db             # SQLite database
├── extract_stock_data.py        # Step 1 - Fetch raw data
├── load_raw_data_db.py          # Step 2 - Clean + load raw data to SQLite
├── transform_load_top100.py     # Step 3 - Filter top 100 stocks daily
├── create_index_composition.py  # Step 4 - Build and rebalance custom index
├── load_index_metrics_excel.py  # Step 5 - Export metrics to Excel
├── requirements.txt             # Project dependencies
└── README.md                    # This file``


## Workflow Overview
### 1. Extract Raw Stock Data
##### File: extract_stock_data.py

- Reads tickers from constituents.csv or calls Polygon API dynamically using:
    ##### get_active_tickers_from_api()
- Fetches daily Close_Price and Market_Cap for the past 30 days.

- Saves raw output as JSON or intermediate CSV.

### 2. Load into SQLite Database
##### File: load_raw_data_db.py

- Cleans the raw data.

- Loads records into:

    Table: stock_prices

### 3. Filter - Top 100 Stocks Daily
##### File: transform_load_top100.py

- Sorts all tickers by Market Cap per Date.

- Selects the top 100 tickers for each day.

- Saves results to:

    Table: transformed_top100_tickers

### 4. Transformation - Create Daily Rebalanced Index
#### File: create_index_composition.py

- Builds a daily equal-weighted index using tickers from transformed_top100_tickers.

- Detects and logs daily rebalances.

- Tracks:

    - Index value

    - Daily % returns

    - Cumulative returns

    - Best/Worst performing days

    - Composition changes (tickers added/removed)

- Saves all data to:

    Table: custom_index_daily_rebalance

### 5. Export Final Metrics to Excel
##### File: load_index_metrics_excel.py

- Writes various performance metrics and composition data to:
    #### /reports/equal_weighted_index_data.xlsx

- Spreadsheets include:

    - index_performance

    - daily_composition

    - composition_changes

    - summary_metrics

## Installation

### Install Python Dependencies
pip install -r requirements.txt


### Configuration
Update config/config.ini with your settings:

##### [params]
base_url = https://api.polygon.io

api_key = YOUR_POLYGON_API_KEY

##### [db_params]

table_stock_prices = stock_prices

table_top100_stocks = transformed_top100_tickers

table_custom_index = custom_index_daily_rebalance

## Execution Order

python extract_stock_data.py           # Step 1 - Fetch from Polygon.io
python load_raw_data_db.py            # Step 2 - Clean & store to DB
python transform_load_top100.py       # Step 3 - Filter top 100 per day
python create_index_composition.py    # Step 4 - Compute index
python load_index_metrics_excel.py    # Step 5 - Export Excel report

## Example Output Preview
### Excel File: reports/equal_weighted_index_data.xlsx

## Excel Sheet Descriptions

| Sheet Name          | Description                                             |
|---------------------|---------------------------------------------------------|
| `index_performance` | Daily index values, % returns, cumulative returns       |
| `daily_composition` | Ticker list for each day’s index                        |
| `composition_changes` | Tickers added/removed during daily rebalancing       |
| `summary_metrics`   | Best/worst day, final return, number of rebalances      |

## Requirements
See requirements.txt for pinned versions. Core packages:

- pandas

- requests

- aiohttp

- openpyxl

- sqlite3 (built-in)

- pathlib, operator (standard library)