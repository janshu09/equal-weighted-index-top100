import configparser
import logging
import sqlite3
from pathlib import Path
import pandas as pd

# Connect to DB and read data
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
test_config = configparser.ConfigParser()
test_config.read(Path("../config/config.ini"))
stock_db = Path("../../SQLite_DBs/stock_data.db")
table_name = test_config.get('db_params', 'table_top100_stocks')

conn = sqlite3.connect(stock_db)
df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
df['Date'] = pd.to_datetime(df['Date'])
df.sort_values(['Date', 'Ticker'], inplace=True)

# Prepare for iteration
grouped = df.groupby('Date')
index_data = []

initial_index = 100.0
prev_index = initial_index
cumulative_return = 1.0
prev_tickers = set()
prev_prices = {}

best_day = {'date': None, 'return': float('-inf')}
worst_day = {'date': None, 'return': float('inf')}

# Process day-by-day
for date, group in grouped:
    current_tickers = set(group['Ticker'])
    current_prices = dict(zip(group['Ticker'], group['Close_Price']))

    # Detect rebalance
    is_rebalanced = current_tickers != prev_tickers
    added = sorted(list(current_tickers - prev_tickers))
    removed = sorted(list(prev_tickers - current_tickers))
    composition_change_count = len(added) + len(removed)

    # Calculate return only on shared tickers
    if prev_tickers:
        shared = current_tickers & prev_tickers
        daily_returns = []
        for ticker in shared:
            if ticker in prev_prices and prev_prices[ticker] != 0:
                ret = (current_prices[ticker] - prev_prices[ticker]) / prev_prices[ticker]
                daily_returns.append(ret)
        avg_return = sum(daily_returns) / len(daily_returns) if daily_returns else 0
    else:
        avg_return = 0

    # Update index
    new_index = prev_index * (1 + avg_return)
    daily_percent_return = (new_index - prev_index) / prev_index if prev_index != 0 else 0
    cumulative_return *= (1 + daily_percent_return)

    # Track best/worst
    if daily_percent_return > best_day['return']:
        best_day = {'date': date, 'return': daily_percent_return}
    if daily_percent_return < worst_day['return']:
        worst_day = {'date': date, 'return': daily_percent_return}

    # Store row
    index_data.append({
        'Date': date,
        'Equal_Weighted_Index': round(new_index, 4),
        'Daily_Percent_Return': round(daily_percent_return * 100, 4),
        'Cumulative_Return': round((cumulative_return - 1) * 100, 4),
        'Constituent_Tickers': ','.join(sorted(current_tickers)),
        'Rebalance_Date': date.strftime('%Y-%m-%d') if is_rebalanced else '',
        'Tickers_Added': ','.join(added) if is_rebalanced else '',
        'Tickers_Removed': ','.join(removed) if is_rebalanced else '',
        'Composition_Change_Count': composition_change_count if is_rebalanced else 0
    })

    # Update trackers
    prev_tickers = current_tickers
    prev_prices = current_prices
    prev_index = new_index

# Add final summary row
aggregate_return = index_data[-1]['Cumulative_Return']
summary_row = pd.DataFrame([{
    'Date': 'SUMMARY',
    'Equal_Weighted_Index': '',
    'Daily_Percent_Return': '',
    'Cumulative_Return': round(aggregate_return, 4),
    'Constituent_Tickers': '',
    'Rebalance_Date': '',
    'Tickers_Added': '',
    'Tickers_Removed': '',
    'Composition_Change_Count': '',
    'Best_Performing_Day': best_day['date'].strftime('%Y-%m-%d'),
    'Worst_Performing_Day': worst_day['date'].strftime('%Y-%m-%d'),
    'Aggregate_Return': round(aggregate_return, 4)
}])

# Finalize output
index_df = pd.DataFrame(index_data)
final_df = pd.concat([index_df, summary_row], ignore_index=True)

# Convert datetime columns to string to avoid binding errors
datetime_cols = ['Date', 'Best_Performing_Day', 'Worst_Performing_Day']
for col in datetime_cols:
    if col in final_df.columns:
        final_df[col] = final_df[col].astype(str)

# Save to SQLite and CSV
final_df.to_sql('custom_index_daily_rebalance', conn, if_exists='replace', index=False)
final_df.to_csv('custom_index_daily_rebalance.csv', index=False)

#conn.close()
logging.info("Daily rebalanced custom index created and saved.")