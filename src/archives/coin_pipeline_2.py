from config import *
from utils.db_utils import *
from financial_database.backend.archives.calc_utils import *
from dotenv import load_dotenv
import os
import json 

load_dotenv(override=True)
gc_api_key = os.getenv('GECKO_API') 
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
# get tickers price
top_n_tickers_by_mc = 70
checkpoint_file_path = CHECKPOINT_JSON_PATH+'/coin_calc_pipeline.json'
coint_csv_path = COINT_CSV_PATH+'/coin_calc_pipeline_coint.csv'
signal_csv_path = SIGNAL_CSV_PATH+'/coin_calc_pipeline_signal.csv'

conn = connect_to_db(DB_NAME, DB_HOST, DB_USERNAME, DB_PASSWORD)
query = f"""
with top_tickers as (
select distinct symbol, market_cap
from coin_overview 
where market_cap is not null
order by market_cap desc
limit {top_n_tickers_by_mc})
 
select a.*
from coin_historical_price a 
join top_tickers b 
on a.symbol=b.symbol
where date >= '2022-01-01'
order by market_cap desc, date
"""
df = pd.read_sql(query, conn)
conn.close()

df = df.drop_duplicates(subset=['date', 'symbol'])
price_df = df.pivot(index='date', columns='symbol', values='close')
price_df.fillna(-1, inplace=True)
price_df.reset_index(inplace=True)

# # get coint
coint_df = save_multi_pairs_rolling_coint(price_df, None, checkpoint_file_path, coint_csv_path)
# coint_df = pd.read_csv(coint_csv_path) # for debug 

# insert coint to db
coint_df.columns = coint_df.columns.str.replace('_p_val$', '', regex=True)
coint_df_melted = pd.melt(coint_df, id_vars=['date'], var_name='pair_name', value_name='value')
coint_df_melted[['symbol1', 'symbol2']] = coint_df_melted['pair_name'].str.split('_', expand=True)
coint_df_melted = coint_df_melted.drop(columns=['pair_name'])
coint_df_melted['window_length'] = ROLLING_COINT_WINDOW
coint_df_melted = coint_df_melted[['date', 'window_length', 'symbol1', 'symbol2', 'value']]

conn = connect_to_db(DB_NAME, DB_HOST, DB_USERNAME, DB_PASSWORD)
create_coin_pair_coint_table(conn)
insert_coin_pair_coint_table(conn, list(coint_df_melted.itertuples(index=False, name=None)))

# get signal 
signal_df = get_signal(coint_df, price_df)
signal_df.to_csv(signal_csv_path)

# insert to db
create_coin_signal_table(conn)
insert_coin_signal_table(conn, list(signal_df.itertuples(index=False, name=None)))

# update api data after calculation
update_coin_signal_final_api_data(conn)
conn.close()







 