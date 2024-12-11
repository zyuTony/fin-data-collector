from config import *
from utils.refactor_signal_calculator import *
from utils.refactor_db_signal_updater import *
from dotenv import load_dotenv
import os
import warnings

warnings.filterwarnings("ignore", category=UserWarning, message="pandas only supports SQLAlchemy connectable")

load_dotenv(override=True)
gc_api_key = os.getenv('GECKO_API') 
DB_USERNAME = os.getenv('RDS_USERNAME') 
DB_PASSWORD = os.getenv('RDS_PASSWORD') 
DB_HOST = os.getenv('RDS_ENDPOINT') 
DB_NAME = 'financial_data'
 
signal_csv_path = SIGNAL_CSV_PATH+'/stonewell_signal.csv'

db = coin_stonewell_signal_updater(DB_NAME, DB_HOST, DB_USERNAME, DB_PASSWORD)
db.connect()
df = db.fetch_input_data(top_n_tickers=50)
 
calculator = stonewell_signal_calculator(df, signal_csv_path)
output_df = calculator.calculate_data()
signal_df = calculator.calculate_signal(output_df)

db.insert_signal_data_table(signal_df)
db.close()




 