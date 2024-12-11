from config import *
from utils.db_utils import *
from utils.gecko_utils import *
from dotenv import load_dotenv
import os
import json
import requests
from datetime import datetime, timedelta

load_dotenv(override=True)
gc_api_key = os.getenv('GECKO_API') 
DB_USERNAME = os.getenv('RDS_USERNAME') 
DB_PASSWORD = os.getenv('RDS_PASSWORD') 
DB_HOST = os.getenv('RDS_ENDPOINT') 
DB_NAME = 'financial_data'

'''
Coin Price Refresh Pipeline
Cadence: AUTOMATIC DAILY
  1. Download hist price json from coin gecko
  2. Insert data into DB
'''

# fetch latest 5 days data since already have previous data
end_date = datetime.now() 
start_date = end_date - timedelta(days=5)
unix_end = get_unix_timestamp(end_date.strftime('%Y-%m-%d'))
unix_start = get_unix_timestamp(start_date.strftime('%Y-%m-%d'))
print(f"---- Download coin data from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")

# get top coins
symbols_ranking = []
for page_num in range(1, 6):
    symbols_ranking.extend(pull_coin_list_ranking(gc_api_key, page_num))

with open(GECKO_JSON_PATH+'/mapping/top_symbol_by_mc.json', 'w') as file:
    json.dump(symbols_ranking, file, indent=4)
 
ids = [item["id"] for item in symbols_ranking][:300]
symbols = [item["symbol"].upper() for item in symbols_ranking][:300]
# print(f"start working on {symbols}") # for debug

# download jsons
for id, symbol in zip(ids, symbols):
    current_start = unix_start
    all_data = []
    while current_start < unix_end:
        current_end = min(current_start + DAYS_PER_API_LIMIT * 24 * 60 * 60, unix_end)
        try:
            url = f"https://pro-api.coingecko.com/api/v3/coins/{id}/ohlc/range?vs_currency=usd&from={current_start}&to={current_end}&interval=daily"
            headers = {
                "accept": "application/json",
                "x-cg-pro-api-key": gc_api_key
            }            
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                all_data.extend(response.json())  

                # print(f"Downloaded data for {symbol} from {current_start} to {current_end}") # for debug
            else:
                print(f"Failed to download data for {symbol} from {current_start} to {current_end}: {response.status_code} {response.text}")
            
            current_start = current_end + 1
        except Exception as e:
            print(f"Exception occurred while downloading data for {symbol} from {current_start} to {current_end}: {e}")
            break

    with open(GECKO_DAILY_JSON_PATH+f'/{symbol}.json', 'w') as file:
        json.dump(all_data, file, indent=4)
    # print(f"Saved full data for {symbol} to {symbol}.json")# for debug
print('---- Downloads complete!')
# insert to DB
# price data
conn = connect_to_db(DB_NAME, DB_HOST, DB_USERNAME, DB_PASSWORD)
create_coin_historical_price_table(conn)
print(f'---- Inserting coin price data...')
for filename in os.listdir(GECKO_DAILY_JSON_PATH):
    if filename.endswith('.json'):
        file_path = os.path.join(GECKO_DAILY_JSON_PATH, filename)
        insert_coin_historical_price_table(conn, file_path)
print(f'---- Insertion complete!')

# overview
create_coin_overview_table(conn)
print(f'---- Inserting coin overview info...')
insert_coin_overview_table(conn, GECKO_JSON_PATH+'/mapping/top_symbol_by_mc.json')
print(f'---- Insertion complete!')

conn.close()