import configparser
import logging
import sqlite3
from pathlib import Path
import pandas as pd

# Connect to the SQLite database
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
test_config = configparser.ConfigParser()
test_config.read(Path("../config/config.ini"))
stock_db = Path("../../SQLite_DBs/stock_data.db")
conn = sqlite3.connect(stock_db)
excel_file = Path("../reports/equal_weighted_index_data.xlsx")
excel_file.parent.mkdir(parents=True, exist_ok=True)

def index_performance():
    """Fetch Relevant Fields from 'custom_index_daily_rebalance' DB and write data to
    index_performance spreadsheet"""
    query = """
    SELECT 
        Date, 
        Equal_Weighted_Index, 
        Daily_Percent_Return, 
        Cumulative_Return
    FROM custom_index_daily_rebalance
    WHERE Date != 'SUMMARY'
    ORDER BY Date
    """

    df = pd.read_sql_query(query, conn)
    #conn.close()

    # Ensure numeric formatting
    df['Equal_Weighted_Index'] = pd.to_numeric(df['Equal_Weighted_Index'], errors='coerce')
    df['Daily_Percent_Return'] = pd.to_numeric(df['Daily_Percent_Return'], errors='coerce')
    df['Cumulative_Return'] = pd.to_numeric(df['Cumulative_Return'], errors='coerce')

    # Round for better Excel display
    df = df.round({
        'Equal_Weighted_Index': 2,
        'Daily_Percent_Return': 4,
        'Cumulative_Return': 4
    })

    # Rename columns for Excel clarity
    df.rename(columns={
        'Equal_Weighted_Index': 'Equal Weighted Index Value',
        'Daily_Percent_Return': 'Daily % Return',
        'Cumulative_Return': 'Cumulative Return'
    }, inplace=True)

    # Export to Excel
    sheet_name = 'index_performance'

    with pd.ExcelWriter(excel_file, engine='openpyxl', mode='w') as writer:
        df.to_excel(writer, sheet_name=sheet_name, index=False)

    logging.info(f"Data exported to sheet '{sheet_name}' in '{excel_file}'")


def daily_composition():
    """Fetch Relevant Fields from 'custom_index_daily_rebalance' DB and write data to
        daily_composition spreadsheet"""
    query = """
            SELECT
                Date, Constituent_Tickers
            FROM custom_index_daily_rebalance
            WHERE Date != 'SUMMARY' \
            """

    df_composition = pd.read_sql_query(query, conn)
    #conn.close()

    # --- Step 2: Append to existing Excel file as new sheet ---
    sheet_name = "daily_composition"

    with pd.ExcelWriter(excel_file, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
        df_composition.to_excel(writer, sheet_name=sheet_name, index=False)

    logging.info(f"Sheet '{sheet_name}' added to Excel file: {excel_file}")

def composition_changes():
    """Fetch Relevant Fields from 'custom_index_daily_rebalance' DB and write data to
            composition_changes spreadsheet"""
    query = """
            SELECT
                Date, Tickers_Added, Tickers_Removed, Constituent_Tickers
            FROM custom_index_daily_rebalance
            WHERE Date != 'SUMMARY'
            ORDER BY Date \
            """

    df = pd.read_sql_query(query, conn)
    #conn.close()

    # Clean and filter for rebalancing days
    df_rebalance = df[df['Tickers_Added'].notnull() & (df['Tickers_Added'] != '')].copy()

    # Compute intersection with previous day
    df_rebalance['Intersection_With_Previous_Day'] = ''

    prev_constituents = set()
    for i, row in df_rebalance.iterrows():
        current_constituents = set(row['Constituent_Tickers'].split(','))
        intersection = current_constituents & prev_constituents if prev_constituents else set()
        df_rebalance.at[i, 'Intersection_With_Previous_Day'] = ','.join(sorted(intersection))
        prev_constituents = current_constituents

    # Select and rename columns
    df_output = df_rebalance[[
        'Date', 'Tickers_Added', 'Tickers_Removed', 'Intersection_With_Previous_Day'
    ]].rename(columns={
        'Date': 'Rebalance_Date',
        'Intersection_With_Previous_Day': 'Intersection_with_Previous_Day'
    })

    # Write to Excel: append as new sheet
    sheet_name = 'composition_changes'

    with pd.ExcelWriter(excel_file, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
        df_output.to_excel(writer, sheet_name=sheet_name, index=False)

    logging.info(f"Sheet '{sheet_name}' added to {excel_file}")

def summary_metrics():
    """Fetch Relevant Fields from 'custom_index_daily_rebalance' DB and write data to
                summary_metrics spreadsheet"""
    query = """
            SELECT
                Date, Daily_Percent_Return, Cumulative_Return, Tickers_Added, Tickers_Removed
            FROM custom_index_daily_rebalance
            WHERE Date != 'SUMMARY' \
            """

    df = pd.read_sql_query(query, conn)
    #conn.close()

    # --- Total number of composition changes ---
    num_changes = df[(df['Tickers_Added'].notnull()) & (df['Tickers_Added'] != '')].shape[0]

    # --- Best and worst performing days ---
    df['Daily_Percent_Return'] = pd.to_numeric(df['Daily_Percent_Return'], errors='coerce')
    best_day_row = df.loc[df['Daily_Percent_Return'].idxmax()]
    worst_day_row = df.loc[df['Daily_Percent_Return'].idxmin()]

    # --- Aggregate return (last cumulative return - 1) ---
    df['Cumulative_Return'] = pd.to_numeric(df['Cumulative_Return'], errors='coerce')
    aggregate_return = df['Cumulative_Return'].iloc[-1] - 1  # Assuming it's in multiplier format

    # --- Prepare summary as DataFrame ---
    summary_data = {
        "Metric": [
            "Total Composition Changes",
            "Best Performing Day",
            "Best Day % Return",
            "Worst Performing Day",
            "Worst Day % Return",
            "Aggregate Return"
        ],
        "Value": [
            num_changes,
            best_day_row['Date'],
            round(best_day_row['Daily_Percent_Return'] * 100, 2),
            worst_day_row['Date'],
            round(worst_day_row['Daily_Percent_Return'] * 100, 2),
            round(aggregate_return * 100, 2)
        ]
    }

    summary_df = pd.DataFrame(summary_data)

    # --- Write to Excel as new sheet ---
    sheet_name = 'summary_metrics'

    with pd.ExcelWriter(excel_file, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
        summary_df.to_excel(writer, sheet_name=sheet_name, index=False)

    logging.info(f"Summary metrics exported to sheet '{sheet_name}' in '{excel_file}'")


if __name__ == '__main__':
    index_performance()
    daily_composition()
    composition_changes()
    summary_metrics()


