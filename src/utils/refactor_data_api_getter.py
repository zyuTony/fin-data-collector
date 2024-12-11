from abc import ABC, abstractmethod
import json
from config import *
import logging
from datetime import datetime
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import requests
import time
import os
from binance.client import Client

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)8s | %(message)s',
    datefmt='%Y-%m-%d %H:%M' #datefmt='%Y-%m-%d %H:%M:%S'
)
 
class api_getter(ABC): 
    def __init__(self, api_key, data_save_path):
        self.api_key = api_key
        self.data_save_path = data_save_path
    
    @abstractmethod
    def download_data(self):
        pass

"""COIN GECKO"""    
class coin_gecko_daily_ohlc_api_getter(api_getter):
    
    def __init__(self, api_key, data_save_path, start_date, end_date):
        super().__init__(api_key, data_save_path)
        self.num_download_symbols = 300 
        self.start_date = start_date if start_date is not None else datetime.strptime('2018-02-10', '%Y-%m-%d')
        self.end_date = end_date if end_date is not None else datetime.now() 
        self.overview_save_path = GECKO_JSON_PATH+'/mapping/top_symbol_by_mc.json'
        
    def _get_unix_from_date_object(self, date_object):
        return int(date_object.timestamp())
       
    def _pull_coin_list_ranking(self, page_num):
        url = f"https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=250&page={page_num}"
        headers = {
            "accept": "application/json",
            "x-cg-pro-api-key": self.api_key
        }
        max_retries = 3
        retry_delay = 5  # seconds

        for attempt in range(max_retries):
            try:
                response = requests.get(url, headers=headers)
                response.raise_for_status()  # Raise an exception for bad status codes
                data = response.json()

                with open(self.overview_save_path, 'w') as file:
                    json.dump(data, file, indent=4)
                
                return data
            except (ConnectionError, Timeout, TooManyRedirects, requests.exceptions.RequestException) as e:
                if attempt < max_retries - 1:
                    logging.warning(f"Attempt {attempt + 1} failed: {e}. Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    logging.error(f"All {max_retries} attempts failed: {e}")
                    return None
 
    def _get_download_symbol_list(self):
        # get top coins
        symbols_ranking = []
        for page_num in range(1, 4): # pull top 1500 coins just in case
            symbols_ranking.extend(self._pull_coin_list_ranking(page_num))

        ids = [item["id"] for item in symbols_ranking][:self.num_download_symbols]
        symbols = [item["symbol"].upper() for item in symbols_ranking][:self.num_download_symbols]
        return ids, symbols
    
    def _download_single_symbol(self, id, symbol):
        unix_start = self._get_unix_from_date_object(self.start_date)
        unix_end = self._get_unix_from_date_object(self.end_date)
        all_data = []    
        current_start = unix_start
        while current_start < unix_end:
            current_end = min(current_start + DAYS_PER_API_LIMIT * 24 * 60 * 60, unix_end)
            try:
                url = f"https://pro-api.coingecko.com/api/v3/coins/{id}/ohlc/range?vs_currency=usd&from={current_start}&to={current_end}&interval=daily"
                headers = {
                    "accept": "application/json",
                    "x-cg-pro-api-key": self.api_key
                }            
                response = requests.get(url, headers=headers)
                if response.status_code == 200:
                    all_data.extend(response.json())  
                    logging.debug(f"Downloaded data for {symbol} from {current_start} to {current_end}")
                else:
                    logging.error(f"Failed to download data for {symbol} from {current_start} to {current_end}: {response.status_code} {response.text}")
                
                current_start = current_end + 1
            except Exception as e:
                logging.exception(f"Exception occurred while downloading data for {symbol} from {unix_start} to {current_end}: {e}")
                break
        return all_data

    def download_data(self):
        ids, symbols = self._get_download_symbol_list()
        
        logging.debug(f"start downloading symbol list:{symbols}") 
        
        for id, symbol in zip(ids, symbols):
            all_data = self._download_single_symbol(id, symbol)
            with open(self.data_save_path+f'/{symbol}.json', 'w') as file:
                json.dump(all_data, file, indent=4)
            logging.info(f"Saved full data for {symbol} to {symbol}.json")
         
class coin_gecko_hourly_ohlc_api_getter(coin_gecko_daily_ohlc_api_getter):     
    '''slight change of download_data from daily ohlc api'''
    def _download_single_symbol(self, id, symbol):
        unix_start = self._get_unix_from_date_object(self.start_date)
        unix_end = self._get_unix_from_date_object(self.end_date)
        all_data = []    
        current_start = unix_start
        while current_start < unix_end:
            current_end = min(current_start + DAYS_PER_API_LIMIT_HOURLY * 24 * 60 * 60, unix_end)
            try:
                url = f"https://pro-api.coingecko.com/api/v3/coins/{id}/ohlc/range?vs_currency=usd&from={current_start}&to={current_end}&interval=hourly"
                headers = {
                    "accept": "application/json",
                    "x-cg-pro-api-key": self.api_key
                }            
                response = requests.get(url, headers=headers)
                if response.status_code == 200:
                    all_data.extend(response.json())  
                    logging.debug(f"Downloaded data for {symbol} from {current_start} to {current_end}")
                else:
                    logging.error(f"Failed to download data for {symbol} from {current_start} to {current_end}: {response.status_code} {response.text}")
                
                current_start = current_end + 1
            except Exception as e:
                logging.exception(f"Exception occurred while downloading data for {symbol} from {unix_start} to {current_end}: {e}")
                break
        return all_data

'''BINANCE'''
class binance_ohlc_api_getter(coin_gecko_daily_ohlc_api_getter):
    '''Binance api data download that include volume data'''
    def __init__(self, api_key, api_secret, data_save_path, interval, start_date, end_date):
        super().__init__(api_key, data_save_path, None, None)
        self.num_download_symbols = TOP_N_COINS_DOWNLOADED_FROM_BINANCE
        self.api_secret = api_secret
        self.client = Client(self.api_key, self.api_secret)
        self.interval = interval
        self.start_date = start_date
        self.end_date = end_date
        
    def _download_single_symbol(self, symbol):
        try:
            # Get data and save to JSON
            symbol = symbol+'USDT'
            ticker_data = self.client.get_historical_klines(symbol, self.interval, self.start_date, self.end_date)
            with open(f'{self.data_save_path}/{self.interval.split("_")[-1]}/{symbol}.json', 'w') as file:
                json.dump(ticker_data, file, indent=4)
            logging.info(f'Downloaded {symbol}')
            return 1
        except Exception as e:
            logging.error(f"Error downloading {symbol}: {e}")
            return -1

    def download_data(self): 
        _, symbols = self._get_download_symbol_list()
        for symbol in symbols:
            self._download_single_symbol(symbol)
            
        
'''ALPHA VANTAGE'''
class avan_stock_daily_ohlc_api_getter(api_getter):
    
    def __init__(self, api_key, data_save_path, start_date, end_date, additional_tickers=None):
        super().__init__(api_key, data_save_path)
        self.num_download_symbols = TOP_N_STOCKS_DOWNLOADED
        self.top_symbols_list_path = SEC_STOCK_TICKERS
        self.start_date = None
        self.end_date = None 
        self.additional_tickers = additional_tickers
       
    def _get_download_symbol_list(self):
        with open(self.top_symbols_list_path, 'r') as file:
            data = json.load(file)
        symbols = []
        for _, value in data.items():
            symbols.append(value["ticker"])
        if self.additional_tickers:
            symbols.extend(self.additional_tickers)
        return symbols[:self.num_download_symbols]

    def download_data(self):
        symbols = self._get_download_symbol_list()
        for symbol in symbols:
            url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol={symbol}&outputsize=full&apikey={self.api_key}'

            response = requests.get(url)
            json_file_path = self.data_save_path + f'/{symbol}.json'
            if response.status_code == 200:
                data = response.json()
                with open(json_file_path, 'w') as file:
                    json.dump(data, file, indent=4)
                logging.info(f"{symbol} saved to {json_file_path}")
            else:
                logging.error(f"Error fetching {symbol}: {response.status_code}")
            time.sleep(AVAN_SLEEP_TIME)      

class avan_stock_selected_daily_ohlc_api_getter(api_getter):
    
    def __init__(self, api_key, data_save_path, start_date, end_date, symbols):
        super().__init__(api_key, data_save_path)
        self.start_date = None
        self.end_date = None 
        self.symbols = symbols

    def download_data(self):
        for symbol in self.symbols:
            url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol={symbol}&outputsize=full&apikey={self.api_key}'

            response = requests.get(url)
            json_file_path = self.data_save_path + f'/{symbol}.json'
            if response.status_code == 200:
                data = response.json()
                with open(json_file_path, 'w') as file:
                    json.dump(data, file, indent=4)
                logging.info(f"{symbol} saved to {json_file_path}")
            else:
                logging.error(f"Error fetching {symbol}: {response.status_code}")
            time.sleep(AVAN_SLEEP_TIME)

class avan_stock_overview_api_getter(avan_stock_daily_ohlc_api_getter):
    def download_data(self):
        symbols = self._get_download_symbol_list()
        for symbol in symbols:
            url = f'https://www.alphavantage.co/query?function=OVERVIEW&symbol={symbol}&apikey={self.api_key}'

            response = requests.get(url)
            json_file_path = self.data_save_path + f'/{symbol}.json'
            if response.status_code == 200:
                data = response.json()
                with open(json_file_path, 'w') as file:
                    json.dump(data, file, indent=4)
                logging.info(f"{symbol} saved to {json_file_path}")
            else:
                logging.error(f"Error fetching {symbol}: {response.status_code}")
            time.sleep(AVAN_SLEEP_TIME)  
            
class avan_stock_income_statement_api_getter(avan_stock_daily_ohlc_api_getter):
    def download_data(self):
        symbols = self._get_download_symbol_list()
        for symbol in symbols:
            url = f'https://www.alphavantage.co/query?function=INCOME_STATEMENT&symbol={symbol}&apikey={self.api_key}'

            response = requests.get(url)
            json_file_path = self.data_save_path + f'/{symbol}.json'
            if response.status_code == 200:
                data = response.json()
                with open(json_file_path, 'w') as file:
                    json.dump(data, file, indent=4)
                logging.info(f"{symbol} saved to {json_file_path}")
            else:
                logging.error(f"Error fetching {symbol}: {response.status_code}")
            time.sleep(AVAN_SLEEP_TIME)  

class avan_stock_balance_sheet_api_getter(avan_stock_daily_ohlc_api_getter):
    def download_data(self):
        symbols = self._get_download_symbol_list()
        for symbol in symbols:
            url = f'https://www.alphavantage.co/query?function=BALANCE_SHEET&symbol={symbol}&apikey={self.api_key}'

            response = requests.get(url)
            json_file_path = self.data_save_path + f'/{symbol}.json'
            if response.status_code == 200:
                data = response.json()
                with open(json_file_path, 'w') as file:
                    json.dump(data, file, indent=4)
                logging.info(f"{symbol} saved to {json_file_path}")
            else:
                logging.error(f"Error fetching {symbol}: {response.status_code}")
            time.sleep(AVAN_SLEEP_TIME)  
            
class avan_stock_cash_flow_api_getter(avan_stock_daily_ohlc_api_getter):
    def download_data(self):
        symbols = self._get_download_symbol_list()
        for symbol in symbols:
            url = f'https://www.alphavantage.co/query?function=CASH_FLOW&symbol={symbol}&apikey={self.api_key}'

            response = requests.get(url)
            json_file_path = self.data_save_path + f'/{symbol}.json'
            if response.status_code == 200:
                data = response.json()
                with open(json_file_path, 'w') as file:
                    json.dump(data, file, indent=4)
                logging.info(f"{symbol} saved to {json_file_path}")
            else:
                logging.error(f"Error fetching {symbol}: {response.status_code}")
            time.sleep(AVAN_SLEEP_TIME)  
            
class avan_stock_economic_api_getter(api_getter):
    def __init__(self, api_key, data_save_path):
        super().__init__(api_key, data_save_path) 
        
    def download_data(self):
        functions = ['REAL_GDP', 'FEDERAL_FUNDS_RATE', 'CPI', 'INFLATION', 
                     'RETAIL_SALES', 'DURABLES', 'UNEMPLOYMENT', 'NONFARM_PAYROLL']
        intervals = ['quarterly', 'monthly', 'monthly' , None, None, None, None, None]
        
        for function, interval in zip(functions, intervals):
            if interval is None:
                url = f"https://www.alphavantage.co/query?function={function}&apikey={self.api_key}"
            url = f"https://www.alphavantage.co/query?function={function}&interval={interval}&apikey={self.api_key}"
            response = requests.get(url)
            json_file_path = self.data_save_path + f'/{function}.json'
            if response.status_code == 200:
                data = response.json()
                with open(json_file_path, 'w') as file:
                    json.dump(data, file, indent=4)
                logging.info(f"{function} saved to {json_file_path}")
            else:
                logging.error(f"Error fetching {function}: {response.status_code}")
            time.sleep(AVAN_SLEEP_TIME)  

    def aggregate_economic_data(self):
        economic_data = {}
        for filename in os.listdir(self.data_save_path):
            if filename.endswith('.json'):
                with open(os.path.join(self.data_save_path, filename), 'r') as file:
                    data = json.load(file)
                    col_name = os.path.splitext(filename)[0]  # Use filename without extension as column name
                    if 'data' in data:
                        for entry in data['data']:
                            date = entry.get('date')
                            value = entry.get('value')
                            if date and value:
                                if date not in economic_data:
                                    economic_data[date] = {}
                                economic_data[date][col_name] = value
        
        # Save aggregated data as JSON
        output_file = os.path.join(self.data_save_path, 'aggregated_economic_data.json')
        with open(output_file, 'w') as f:
            json.dump(economic_data, f, indent=4)
        
        logging.info(f"Aggregated economic data saved to {output_file}")
        return economic_data
        