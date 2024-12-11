from config import *
from utils.db_utils import *
from utils.avan_utils import *

from dotenv import load_dotenv
import os
import json
 

load_dotenv(override=True)
bn_api_key = os.getenv('BINANCE_API')  
bn_api_secret = os.getenv('BINANCE_SECRET')  
cmc_api_key = os.getenv('CMC_API')  
avan_api_key = os.getenv('ALPHA_VANTAGE_PREM_API') 

DB_USERNAME = os.getenv('RDS_USERNAME') 
DB_PASSWORD = os.getenv('RDS_PASSWORD') 
DB_HOST = os.getenv('RDS_ENDPOINT') 
DB_NAME = 'financial_data'

'''
Stock Overview Refresh Pipeline
Cadence: AUTOMATIC WEEKLY
  1. Download stock overview json from AVAN
  2. Insert overview data into DB
'''
# download json
top_n_stocks = 2000
interval = 'DAILY'
with open(SEC_STOCK_TICKERS, 'r') as file:
        data = json.load(file)
tickers = []
print(f'---- Begin downloading Top {top_n_stocks} stocks overviews')
for key, value in data.items():
   tickers.append(value["ticker"])
avan_pull_stocks_overview_json(avan_api_key, tickers[:top_n_stocks])

# insert to db
conn = connect_to_db(DB_NAME, DB_HOST, DB_USERNAME, DB_PASSWORD)
print('---- Download completed! :)')  
print(f'---- Begin stock data insertion to SQL')
create_stock_overview_table(conn)
for filename in os.listdir(AVAN_OVERVIEW_JSON_PATH):
    if filename.endswith('.json'):
        file_path = os.path.join(AVAN_OVERVIEW_JSON_PATH, filename)
        try:
            insert_stock_overview_table(conn, file_path)  
        except Exception as e:
            print(f'Error processing {filename}: {e}')
            continue 
        # print(f'Inserted {filename} overview')
conn.close()
print(f'---- Insertion complete!')