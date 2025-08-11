import configparser
import logging
import sqlite3
from pathlib import Path

class LoadTop100Stocks:
    """Class to Filter Top 100 Stocks Data per Day from Database Table and load to another Table"""
    def __init__(self):
        test_config = configparser.ConfigParser()
        test_config.read(Path("../config/config.ini"))
        self.stock_db = Path("../../SQLite_DBs/stock_data.db")
        self.table_name = test_config.get('db_params', 'table_top100_stocks')

    def fetch_load_top100_stocks(self):
        """To Fetch Top 100 Stocks Data per Day from Database Table and load to another Table"""
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        conn = sqlite3.connect(self.stock_db)
        cursor = conn.cursor()

        # Drop raw data table if exists
        cursor.execute(f'''DROP TABLE IF EXISTS {self.table_name}''')

        # Execute the query to create and populate the new table
        create_table_query = f"""CREATE TABLE {self.table_name} AS
            SELECT Ticker, Date, Close_Price, Market_Cap
            FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY Date ORDER BY Market_Cap DESC) AS RN
                FROM stock_prices
        ) WHERE RN <= 100 ORDER BY Date, RN;"""
        cursor.execute(create_table_query)
        conn.commit()

        #cursor.close()
        #conn.close()

        logging.info(f"Loading Top 100 Stocks Data into {self.table_name} Database")

if __name__ == "__main__":
    LoadTop100Stocks().fetch_load_top100_stocks()