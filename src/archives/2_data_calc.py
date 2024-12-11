from config import *
from financial_database.backend.archives.calc_utils import *
from dotenv import load_dotenv

load_dotenv(override=True)
DB_USERNAME = os.getenv('RDS_USERNAME') 
DB_PASSWORD = os.getenv('RDS_PASSWORD') 
DB_HOST = os.getenv('RDS_ENDPOINT') 
DB_NAME = 'financial_data'

'''
JSON-> CALCULATE rolling coint  **MOST TIME CONSUMING
'''
#coin
coin_price_df = pd.read_csv(COIN_PRICE_CSV)
save_multi_pairs_rolling_coint(coin_price_df, 30, ROLLING_COINT_COIN_CHECKPOINT_FILE, COIN_COINT_RESULT_CSV)

#stock
stock_price_df = pd.read_csv(STOCK_PRICE_CSV)
save_multi_pairs_rolling_coint(stock_price_df, 30, ROLLING_COINT_STOCK_CHECKPOINT_FILE, STOCK_COINT_RESULT_CSV)


'''
CALCULATE signal with formula：
(hist_sig_days/hist_tlt_days) + 2 * (recent2_sig_days/recent2_tlt_days) + 3 * (recent_sig_days/recent_tlt_days)
'''
# coin
coin_coint_df = pd.read_csv(COINT_CSV_PATH+'/final_coint_binance_data.csv')
coin_price_df = pd.read_csv(COIN_PRICE_CSV)
df = get_signal(coin_coint_df, coin_price_df) # COIN_COINT_RESULT_CSV
df.to_csv(COIN_SIGNAL_CSV,index=False)

#stock
stock_coint_df = pd.read_csv(COINT_CSV_PATH+'/final_coint_stock_data_top_100.csv')
stock_price_df = pd.read_csv(STOCK_PRICE_CSV)
df = get_signal(stock_coint_df, stock_price_df) # STOCK_COINT_RESULT_CSV
df.to_csv(STOCK_SIGNAL_CSV,index=False)

 