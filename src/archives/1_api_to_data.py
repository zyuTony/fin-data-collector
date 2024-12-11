import os
import json
from dotenv import load_dotenv 
from binance.client import Client
import pandas as pd
from datetime import datetime
import time
from requests.exceptions import ReadTimeout, ConnectionError
from tqdm import tqdm 
from utils.bn_utils import *
from utils.avan_utils import *
from financial_database.backend.archives.calc_utils import *
from config import *

'''STEPS
1. clean up the checkpoint files to restart download
2. json file downloaded. 
   est time: for 1000 stocks
     avan data     13 minutes
     avan overview 13 minutes
     binance stock 13 minutes
3. coint data run. 
   est time: for n stocks n*(n-1)/2 pairs
   each pair takes 6 sec. easily take 30 days
    
'''
# load env var 
load_dotenv()
bn_api_key = os.getenv('BINANCE_API')  
bn_api_secret = os.getenv('BINANCE_SECRET')  
cmc_api_key = os.getenv('CMC_API')  
avan_api_key = os.getenv('ALPHA_VANTAGE_PREM_API') 

'''BN->JSON'''
start_date='1 Jan, 2020'
end_date='6 Aug, 2024'
top_n_coins=200
interval = Client.KLINE_INTERVAL_1DAY
interval_name = '1DAY'

bn_pull_top_coins_hist_price_json(cmc_api_key, bn_api_key, bn_api_secret, start_date, end_date, interval, interval_name, top_n_coins)

'''AVAN by top MC -> JSON'''
top_n_stocks = 1000
interval = 'DAILY'
# get symbol from sec file
with open(config.SEC_STOCK_TICKERS, 'r') as file:
        data = json.load(file)
tickers = []
for key, value in data.items():
   tickers.append(value["ticker"])

tickers = tickers[:top_n_stocks]
avan_pull_stocks_hist_price_to_json(avan_api_key, interval, tickers)
avan_pull_stocks_overview_json(avan_api_key, tickers)

'''AVAN by tickers -> JSON'''
tickers = ["TXGE", "XOM", "CVX", "COP", "EOG", "EPD", "SLB", "MPC", "PSX", "ET", "OXY", "WMB", "OKE", "VLO", "KMI", "TBN", "LNG", "MPLX", "HES", "FANG", "BKR", "TRGP", "DVN", "HAL", "CQP", "TPL", "EQT", "CTRA", "MRO", "WES", "PAA", "PR", "OVV", "FTI", "APA", "CHK", "CHRD", "DINO", "HESM", "AR", "VNOM", "WFRD", "IEP", "RRC", "SUN", "DTM", "NOV", "MTDR", "SWN", "AM"] # energy
interval = 'DAILY'
avan_pull_stocks_hist_price_to_json(avan_api_key, interval, tickers)
avan_pull_stocks_overview_json(avan_api_key, tickers)

 
'''AVAN JSONs -> CSV'''
symbols = ['AAPL']
combined_df = pd.DataFrame()
for filename in os.listdir(AVAN_DAILY_JSON_PATH):
    if filename.endswith('.json'):
        file_path = os.path.join(AVAN_DAILY_JSON_PATH, filename)
        
        with open(file_path, 'r') as file:
            data = json.load(file)
            symbol = data["Meta Data"]["2. Symbol"]
        
        if symbol in symbols:
            print(f'begin {symbol}')
            df = avan_single_json_append_to_csv(file_path)
            
            if combined_df.empty:
                combined_df = df
            else:
                combined_df = pd.merge(combined_df, df, on='Date', how='outer')

# Sort the dataframe by Date
combined_df.sort_values(by='date', inplace=True)
combined_df.to_csv(STOCK_PRICE_CSV, index=False)