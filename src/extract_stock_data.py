import configparser
import logging
from operator import index
from pathlib import Path
import requests
import time
import csv
import pandas as pd
from datetime import datetime, timedelta, timezone
from aiohttp import ClientResponseError


class StockData:

    def __init__(self):
        test_config = configparser.ConfigParser()
        test_config.read(Path("../config/config.ini"))
        self.source = test_config.get('params', 'ticker_source')
        self.API_KEY = test_config.get('params', 'api_key')
        self.BASE_URL = test_config.get('params', 'base_url')

    def fetch_json(self, session, url, params, retries=3):
        """Fetch the JSON data from the given URL."""
        for attempt in range(retries):
            try:
                response = session.get(url, params=params)
                response.raise_for_status()
                return response.json()
            except ClientResponseError as e:
                logging.warning(f"Attempt {attempt + 1} failed for URL {url} with Error : {e}")
                time.sleep(2 ** attempt)
        return None

    def get_active_tickers_from_api(self, session, LIMIT_TICKERS=100, paginated=False):
        """Get list of active US Stock Tickers from API (paginated)"""
        tickers = []
        stocks = []
        url = f"{self.BASE_URL}/v3/reference/tickers"
        params = {
            "market": "stocks",
            "active": "true",
            "limit": LIMIT_TICKERS,
            "apikey": self.API_KEY
        }

        while True:
            #res = requests.get(url, params=params).json()
            res = self.fetch_json(session, url, params)
            logging.debug(f"Get_Active_Traders API Response :: {res}")
            tickers.extend([item["ticker"] for item in res.get("results", [])])
            stocks.extend([item["Name"] for item in res.get("results", [])])
            logging.info(f"Tickers :: {tickers}")
            if paginated and "next_url" in res:
                url = res.get("next_url", None)
                params = {"apikey": self.API_KEY}
                time.sleep(1)
            else:
                break
        return tickers, stocks

    @staticmethod
    def get_active_tickers_from_csv():
        """Get list of active US Stock Tickers from CSV File (paginated)"""
        tickers = []
        stocks = []
        #logging.info(f"Getting Active Tickers from CSV File :: {path}")
        csv_file = Path("../data/constituents.csv")

        with open(csv_file, mode='r', newline='', encoding='utf-8') as file:
            reader = csv.reader(file)
            next(reader) #Skip 1st Header Row
            for row in reader:
                if row:
                    tickers.append(row[0])
                    stocks.append(row[1])

        logging.info(f"Tickers List from CSV File :: {tickers}")
        return tickers, stocks

    def get_market_cap(self, session, ticker, date):
        """Fetch market cap for given date from API"""
        url = f"{self.BASE_URL}/v3/reference/tickers/{ticker}"
        params = {"date": date,
                  "apikey": self.API_KEY}
        try:
            #res = requests.get(url, params=params).json()
            res = self.fetch_json(session, url, params)
            results = res.get("results", [])
            logging.info(f"Get_Market_Cap API Response for Date {date} for Ticker {ticker} :: {res}")
            if isinstance(results, dict) and "market_cap" in results:
                m_cap = results.get("market_cap", 0)
                logging.debug(f"Get_Market_Cap API Response :: {res.get("results")}")
                logging.info(f"Fetching Market Cap for Ticker {ticker} for {date} :: {m_cap}")
                return m_cap
        except:
            logging.warning(f"Missing 'market_cap' in API response for {ticker} on {date}")
            return 0

    def get_daily_prices(self, session, ticker, from_date, to_date):
        """Fetch daily prices data from API"""
        url = f"{self.BASE_URL}/v2/aggs/ticker/{ticker}/range/1/day/{from_date}/{to_date}"
        params = {
            "adjusted": "true",
            "apiKey": self.API_KEY
        }
        try:
            #res = requests.get(url, params=params).json()
            res = self.fetch_json(session, url, params)
            logging.debug(f"Get_DailyPrices API Response :: {res}")
            return res.get("results", [])
        except:
            return []

    @staticmethod
    def main():
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        logging.info("#### Starting of Data Extraction Part ####")
        combined_data = []

        stock_data = StockData()
        start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        end_date = datetime.now().strftime("%Y-%m-%d")
        logging.info(f"Start_Date :: {start_date} || End_Date :: {end_date}")
        # output_file = "stock_data.csv"
        output_file = Path("../data/stock_data.csv")
        #session = ClientSession()
        session = requests.Session()

        if stock_data.source == 'csv':
            tickers, stocks = stock_data.get_active_tickers_from_csv()
        else:
            tickers, stocks = stock_data.get_active_tickers_from_api(session)

        logging.info(f"Found {len(tickers)} active stock tickers from last 30 days")

        for ticker in tickers:
            stock = stocks[index(ticker)]
            logging.info(f"Fetching Data for Ticker : {ticker} | Stock : {stock}")
            try:
                # Fetching Daily OHLCV Price Data for the Date Range
                daily_data = stock_data.get_daily_prices(session, ticker, start_date, end_date)
                # time.sleep(12)  # Wait for Rate Limit

                if daily_data:
                    for day in daily_data:
                        close_price = day.get("c")
                        logging.info(f"Fetching Close_Price for Ticker {ticker} for {date} :: {close_price}")
                        timestamp_sec = int(day.get("t", 0))
                        date = datetime.fromtimestamp(timestamp_sec / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
                        logging.debug(f"Fetching Market Cap for Ticker {ticker} for {date}")
                        # Fetching Market Cap for the Ticker for the respective date
                        m_cap = stock_data.get_market_cap(session, ticker, date)

                        # market_cap = close_price * shares_outstanding if close_price else None
                        if close_price and m_cap:
                            combined_data.append({
                                "Ticker": ticker,
                                "Security": stock,
                                "Date": date,
                                "Close_Price": round(close_price, 2),
                                "Market_Cap": round(m_cap, 2)
                            })
                        logging.info(f"Combined Output for past 30 days for Ticker {ticker} :: {combined_data[-1]}")
                        time.sleep(1.5)
                time.sleep(3)
            except Exception as ex:
                logging.error(f"Error encountered - {ex}")
                # continue

        logging.info(f"Final Combined Data List :: {combined_data}")
        # Saving to CSV
        df = pd.DataFrame(combined_data)
        df = df.sort_values(by=["Ticker", "Date"])
        df.to_csv(output_file, index=False)
        logging.info(f"Wrote data to File - {output_file}")

# Main Function
if __name__ == "__main__":
    StockData.main()



