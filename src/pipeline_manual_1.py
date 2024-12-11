from config import *
from utils.db_utils import *
from utils.avan_utils import *
from financial_database.backend.archives.calc_utils import *
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
 

'''MANUAL FUNCTION'''
'''
A. get coint by target
1. pull price data from DB for target ticker and comparison tickers
2. Calculate rolling coint for target vs all comparisons
3. Calculate signal
'''
# get tickers price
checkpoint_file_path = CHECKPOINT_JSON_PATH+'/manual_pipeline.json'
coint_csv_path = COINT_CSV_PATH+'/manual_pipeline_coint.csv'
signal_csv_path = SIGNAL_CSV_PATH+'/manual_pipeline_signal.csv'
target_symbol = 'GAP'
sector = 'related to above symbol'
conn = connect_to_db(DB_NAME, DB_HOST, DB_USERNAME, DB_PASSWORD)
query = f"""
select a.*
from stock_historical_price a 
join stock_overview b 
on a.symbol=b.symbol
where sector = {sector}
and date >= '2022-01-01'
order by marketcapitalization desc
"""
df = pd.read_sql(query, conn)
conn.close()

price_df = df.pivot(index='date', columns='symbol', values='close')
price_df.fillna(-1, inplace=True)
price_df.reset_index(inplace=True)

# get coint
coint_df = save_target_symbol_rolling_coint(target_symbol, price_df, None, checkpoint_file_path, coint_csv_path)
# option for getting all possible pairs for top 20 symbols
# top_n_symbols = 20
# coint_df = save_multi_pairs_rolling_coint(price_df, top_n_symbols, checkpoint_file_path, coint_csv_path)

# insert coint to db
coint_df = pd.read_csv(coint_csv_path)
coint_df.columns = coint_df.columns.str.replace('_p_val$', '', regex=True)
coint_df_melted = pd.melt(coint_df, id_vars=['date'], var_name='pair_name', value_name='value')
coint_df_melted[['symbol1', 'symbol2']] = coint_df_melted['pair_name'].str.split('_', expand=True)
coint_df_melted = coint_df_melted.drop(columns=['pair_name'])
coint_df_melted['window_length'] = 120
coint_df_melted = coint_df_melted[['date', 'window_length', 'symbol1', 'symbol2', 'value']]

conn = connect_to_db(DB_NAME, DB_HOST, DB_USERNAME, DB_PASSWORD)
create_stock_pair_coint_table(conn)
insert_stock_pair_coint_table(conn, list(coint_df_melted.itertuples(index=False, name=None)))

# get signal 
signal_df = get_signal(coint_df, price_df)
signal_df.to_csv(signal_csv_path)

# insert to db
create_stock_signal_table(conn)
insert_stock_signal_table(conn, list(signal_df.itertuples(index=False, name=None)))

# update api data after calculation
update_stock_signal_final_api_data(conn)
conn.close()