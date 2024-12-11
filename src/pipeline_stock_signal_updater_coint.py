from config import *
from utils.refactor_signal_calculator import *
from utils.refactor_db_signal_updater import *
from dotenv import load_dotenv
import os
import warnings

warnings.filterwarnings("ignore", category=UserWarning, message="pandas only supports SQLAlchemy connectable")

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
Calculation Refresh Pipeline 1
Cadence: AUTOMATIC DAILY
  1. Calculate rolling coint for top tickers by MC
  2. Calculate signal from rolling coint 
  3. Insert rolling coint to DB
  4. Insert signal to DB
'''

checkpoint_file_path = CHECKPOINT_JSON_PATH+'/calc_pipeline.json'
coint_csv_path = COINT_CSV_PATH+'/calc_pipeline_coint.csv'
signal_csv_path = SIGNAL_CSV_PATH+'/calc_pipeline_signal.csv'

db = stock_coint_db_signal_updater(DB_NAME, DB_HOST, DB_USERNAME, DB_PASSWORD)
db.connect()
df = db.fetch_input_data(top_n_tickers=80)
price_df = db.pivot_price_data(df)

coint_calc = coint_signal_calculator(price_df, checkpoint_file_path, coint_csv_path, signal_csv_path)
coint_df = coint_calc.calculate_data()
# coint_df = pd.read_csv(coint_csv_path)

# insert coint data
db.insert_output_data(coint_calc.transform_data(coint_df))

# calculate signal and insert signal
coint_df.columns = coint_df.columns.str.replace('_p_val$', '', regex=True)
signal_df = coint_calc.calculate_signal(coint_df)
db.insert_signal_data_table(signal_df)

# update api data after calculation
db.insert_api_output_data()

db.close()




 