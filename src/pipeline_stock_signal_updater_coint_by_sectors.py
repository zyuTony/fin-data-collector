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
Calculation Refresh Pipeline 2
Cadence: AUTOMATIC DAILY
  1. Calculate rolling coint for top pairs by segments
  2. Calculate signal from rolling coint 
  3. Insert rolling coint to DB
  4. Insert signal to DB
'''
# get sectors and top tickers 
checkpoint_file_path = CHECKPOINT_JSON_PATH+'/calc_pipeline.json'
coint_csv_path = COINT_CSV_PATH+'/calc_pipeline_coint_by_segment.csv'
signal_csv_path = SIGNAL_CSV_PATH+'/calc_pipeline_signal_by_segment.csv'

db = stock_coint_by_segment_db_signal_updater(DB_NAME, DB_HOST, DB_USERNAME, DB_PASSWORD)
db.connect()
df = db.fetch_input_data(top_n_tickers_by_sectors=50)
price_df = db.pivot_price_data(df)

all_results = pd.DataFrame()
sector_groups = df.groupby('sector')
for sector, group in sector_groups:
    sector_price_df = db.pivot_price_data(group)
    sector_coint_csv_path = f"{os.path.splitext(coint_csv_path)[0]}_{sector}.csv"
    print(f'working on {sector} sector')
    coint_calc = coint_signal_calculator(sector_price_df, checkpoint_file_path, sector_coint_csv_path, signal_csv_path)
    sector_coint_df = coint_calc.calculate_data()
    # join results
    sector_coint_df.set_index('date', inplace=True)
    all_results = pd.concat([all_results, sector_coint_df], axis=1, join='outer')

# save all results
all_results.reset_index(inplace=True)
all_results.to_csv(coint_csv_path, index=False)

# insert coint to db
coint_df = pd.read_csv(coint_csv_path)

# insert coint data
db.insert_output_data(coint_calc.transform_data(coint_df))

# calculate signal and insert signal
coint_df.columns = coint_df.columns.str.replace('_p_val$', '', regex=True)
signal_df = coint_calc.calculate_signal(coint_df)
db.insert_signal_data_table(signal_df)

# update api data after calculation
db.insert_api_output_data()

db.close()