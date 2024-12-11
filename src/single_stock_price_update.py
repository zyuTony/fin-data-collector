from config import *
from dotenv import load_dotenv
import os
from utils.refactor_db_data_updater import *
from utils.refactor_data_api_getter import *

load_dotenv(override=True)
bn_api_key = os.getenv('BINANCE_API')  
bn_api_secret = os.getenv('BINANCE_SECRET')  
cmc_api_key = os.getenv('CMC_API')  
avan_api_key = os.getenv('ALPHA_VANTAGE_PREM_API') 

DB_USERNAME = os.getenv('RDS_USERNAME') 
DB_PASSWORD = os.getenv('RDS_PASSWORD') 
DB_HOST = os.getenv('RDS_ENDPOINT') 
DB_NAME = 'financial_data'

downloader = avan_stock_selected_daily_ohlc_api_getter(
    api_key=avan_api_key,
    data_save_path='./',
    # data_save_path=AVAN_DAILY_JSON_PATH,
    start_date=None,  # not applicable
    end_date=None,  # not applicable
    symbols=["WFIVX"]
)

downloader.download_data()
# single symbol test
symbol = "WFIVX"
url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol={symbol}&outputsize=full&apikey={avan_api_key}'

response = requests.get(url)
json_file_path = f'./{symbol}.json'
if response.status_code == 200:
    data = response.json()
    with open(json_file_path, 'w') as file:
        json.dump(data, file, indent=4) 