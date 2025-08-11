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

- pathlib, operator (standard library

## Index Data Maintenance and Update Strategy
- To ensure the index remains accurate and relevant over time, we adopt the following maintenance approach:


- Daily Rebalancing:
The index is recalculated and rebalanced daily based on the top 100 US stocks by market capitalization, as stored in the transformed_top100_tickers table. This allows the index to reflect the most current market dynamics.


- Automated Data Refresh:
The data extraction scripts are designed to pull the latest pricing and market cap data daily via the Polygon.io API (or CSV fallback). These scripts can be scheduled to run automatically using tools like cron, Airflow, or Autosys.


- Change Tracking:
All composition changes—added/removed tickers—are tracked and stored in the Excel report (composition_changes sheet) along with performance metrics. This provides key data-points into index value evolution.

## Scaling and Future Improvements
As the project grows, here are some suggestions and plans for scaling and enhancing the solution:

### 1. Multi-Source Data Integration
Alternative APIs: Integrate with multiple data providers like Yahoo Finance, Alpha Vantage, or IEX Cloud to avoid single-point dependency.

Fallback Mechanism: Implement logic to switch providers if one fails to respond or returns incomplete data.

### 2. Scaling & Automation Using AWS
- To bring this solution to production scale, AWS services can be used to automate, monitor, and scale the entire pipeline efficiently


    - AWS Lambda (Event-Driven Processing):
Host core functions (e.g., API calls, data transformation, DB updates) as serverless Lambda functions, which can be triggered on a schedule or by events (e.g., new data arrival, file upload).

    - Amazon EventBridge or CloudWatch Scheduler:
Schedule daily index refresh jobs by triggering Lambda functions or ECS tasks without managing any infrastructure manually.

    - AWS S3 (Data Lake Storage):
Store raw CSV files, API responses, and final Excel reports in version-controlled S3 buckets for durable and scalable storage.

    - Amazon RDS (Managed SQLite Alternative):
Use Amazon RDS with PostgreSQL or MySQL to replace local SQLite for multi-user access, better performance, and reliability.

    - AWS Fargate / ECS:
Containerize the entire pipeline and deploy it on ECS with Fargate for scalable, serverless compute with no EC2 management overhead.

    - AWS Athena + Glue (Optional Advanced Analytics):
For larger datasets, use AWS Glue to catalog and transform raw data in S3 and query it with Athena for serverless analytics.

    - Amazon QuickSight (Visualization):
Create interactive dashboards and visualizations of index performance, daily changes, and historical trends.

    - IAM & Secrets Manager:
Manage API keys and DB credentials securely using AWS Secrets Manager and fine-grained access control with IAM.

### 3. Scheduling
- End-to-End Pipeline Automation: Use task schedulers (e.g., cron) or orchestration frameworks (e.g., Apache Airflow) to automate daily data extraction, transformation, and reporting.

- Dockerize the Workflow: Containerizing the entire pipeline makes it portable and production-ready.

### 4. Real-Time and Intraday Updates
- Real-Time Data Feed: Upgrade to premium APIs for intraday or real-time data collection.

- Streaming Architecture: Use message queues (Kafka) and event-driven architecture to respond to market changes as they happen.


### 5. Advanced Analytics
- Benchmark Comparison: Compare custom index performance against major benchmarks like the S&P 500 or Nasdaq.

### 6. Dashboard for Visualization
- Build a lightweight dashboard using tools like Streamlit, Dash, or a React frontend to display:

    - Daily index values

    - Composition heatmaps

    - Performance analytics

