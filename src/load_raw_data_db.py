import configparser
import sqlite3
import pandas as pd
import logging
from pathlib import Path


class SaveRawDataToDb:
    """Class to save raw data into sqlite database"""

    def __init__(self, size=1000):
        """Initialize the class"""
        test_config = configparser.ConfigParser()
        test_config.read(Path("../config/config.ini"))
        self.stock_db = Path("../../SQLite_DBs/stock_data.db")
        self.table_name = test_config.get('db_params', 'table_name')
        self.csv_file = Path("../data/stock_data.csv")
        self.batch_size = size

    def connect_db(self):
        """Connect to database"""
        conn = sqlite3.connect(self.stock_db)
        cursor = conn.cursor()

        #Drop raw data table if exists
        cursor.execute(f'''DROP TABLE IF EXISTS {self.table_name}''')

        #Create Table
        create_table_query = f"""CREATE TABLE IF NOT EXISTS {self.table_name} 
        (Ticker TEXT, Date TEXT, Close_Price REAL, Market_Cap REAL);"""
        cursor.execute(create_table_query)
        conn.commit()
        return conn, cursor

    def clean_chunk(self, chunk):
        """Clean and Prepare Chunk"""

        #Ensure Columns are present
        req_columns = {'Ticker', 'Date', 'Close_Price', 'Market_Price'}
        #logging.info(f"Columns Chunk: {chunk.columns} :: {chunk}")
        # if not req_columns.issubset(set(chunk.columns)):
        #     raise ValueError(f"Missing one or more required columns: {req_columns}")

        #Strip Whitespace from Tickers
        chunk['Ticker'] = chunk['Ticker'].astype(str).str.strip()

        #Convert Date to ISO format (YYYY-MM-DD), coerce errors to NAT
        #chunk["Date"] = pd.to_datetime(chunk["Date"], format="%Y-%m-%d", errors="coerce")

        #Convert Numeric fields
        chunk["Close_Price"] = pd.to_numeric(chunk["Close_Price"], errors="coerce")
        chunk["Market_Cap"] = pd.to_numeric(chunk["Market_Cap"], errors="coerce")

        #Drop Rows with any Missing Invalid Values
        #chunk = chunk.dropna(subset=req_columns)

        #Fill Column Values where Market_Cap is 0
        #chunk = chunk.fillna(method="ffill")
        return chunk

    def insert_batch(self, df_chunk, cursor):
        """Insert chunk into database"""
        records = df_chunk.to_records(index=False)
        cursor.executemany(f'''INSERT INTO {self.table_name} (Ticker, Date, Close_Price, Market_Cap)
        VALUES (?, ?, ?, ?);''', records)

    def save_raw_data_to_db(self, chunk_size=1000):
        """Save raw data into sqlite database in Batches"""
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        logging.info("#### Starting of Data Extraction Part ####")
        conn, cursor = self.connect_db()
        for chunk in pd.read_csv(self.csv_file, chunksize=chunk_size):
            cleaned = self.clean_chunk(chunk)
            if not cleaned.empty:
                self.insert_batch(cleaned, cursor)
                conn.commit()

        #Add Index After Import
        cursor.execute(f'CREATE INDEX IF NOT EXISTS idx_ticker ON {self.table_name} (Ticker);')
        cursor.execute(f'CREATE INDEX IF NOT EXISTS idx_date ON {self.table_name} (Date);')
        conn.commit()
        logging.info(f"Saving Raw Data into {self.table_name} Database")
        #cursor.close()
        #conn.close()
        logging.info(f"CSV data imported, cleaned, and indexed successfully.")

if __name__ == "__main__":
    save_db_data = SaveRawDataToDb()
    save_db_data.save_raw_data_to_db()
