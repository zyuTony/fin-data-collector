from dotenv import load_dotenv
import os
from utils.db_utils import *
import pandas as pd
from config import *

load_dotenv(override=True)
DB_USERNAME = os.getenv('RDS_USERNAME') 
DB_PASSWORD = os.getenv('RDS_PASSWORD') 
DB_HOST = os.getenv('RDS_ENDPOINT') 
DB_NAME = 'financial_data' 
 
 
conn = connect_to_db(DB_NAME, DB_HOST, DB_USERNAME, DB_PASSWORD)

'''
--- RAW JSON -> DB - 0.8s per json; 25 min total
'''
# update stock data
create_stock_historical_price_table(conn)
for filename in os.listdir(AVAN_DAILY_JSON_PATH):
    if filename.endswith('.json'):
        file_path = os.path.join(AVAN_DAILY_JSON_PATH, filename)
        try:
            insert_stock_historical_price_table(conn, file_path)
        except Exception as e:
            print(f'Error processing {filename}: {e}')
            continue
    print(f'Inserted {filename} historical price')

# update stock overview data
create_stock_overview_table(conn)
for filename in os.listdir(AVAN_OVERVIEW_JSON_PATH):
    if filename.endswith('.json'):
        file_path = os.path.join(AVAN_OVERVIEW_JSON_PATH, filename)
        try:
            insert_stock_overview_table(conn, file_path)
            print(f'Inserted {filename} overview')
        except Exception as e:
            print(f'Error processing {filename}: {e}')
            continue 

# update coin data
create_coin_historical_price_table(conn)
for filename in os.listdir(BN_DAILY_JSON_PATH):
    if filename.endswith('.json'):
        file_path = os.path.join(BN_DAILY_JSON_PATH, filename)
        try:
            insert_coin_historical_price_table(conn, file_path)
        except Exception as e:
            print(f'Error processing {filename}: {e}')
            continue
    print(f'Inserted {filename} historical price')

'''
--- ROLLING COINT CSV -> DB - 6000 pairs per min
'''
# stock rolling coint of 120 
df = pd.read_csv(STOCK_COINT_RESULT_CSV)
df.columns = df.columns.str.replace('_p_val$', '', regex=True)
df_melted = pd.melt(df, id_vars=['date'], var_name='pair_name', value_name='value')
df_melted[['symbol1', 'symbol2']] = df_melted['pair_name'].str.split('_', expand=True)
df_melted = df_melted.drop(columns=['pair_name'])
df_melted['window_length'] = 120
df_melted = df_melted[['date', 'window_length', 'symbol1', 'symbol2', 'value']]

create_stock_pair_coint_table(conn)
insert_stock_pair_coint_table(conn, list(df_melted.itertuples(index=False, name=None)))


# coin rolling coint of 120  
df = pd.read_csv(COIN_COINT_RESULT_CSV)
df.columns = df.columns.str.replace('_p_val$', '', regex=True)
df_melted = pd.melt(df, id_vars=['date'], var_name='pair_name', value_name='value')
df_melted[['symbol1', 'symbol2']] = df_melted['pair_name'].str.split('_', expand=True)
df_melted = df_melted.drop(columns=['pair_name'])
df_melted['window_length'] = 120
df_melted = df_melted[['date', 'window_length', 'symbol1', 'symbol2', 'value']]

create_coin_pair_coint_table(conn)
insert_coin_pair_coint_table(conn, list(df_melted.itertuples(index=False, name=None)))


'''
--- SIGNALS CSV -> DB
'''
# coins
create_coin_signal_table(conn)
insert_coin_signal_table(conn, list(pd.read_csv(COIN_SIGNAL_CSV).itertuples(index=False, name=None)))

# stocks
create_stock_signal_table(conn)
insert_stock_signal_table(conn, list(pd.read_csv(STOCK_SIGNAL_CSV).itertuples(index=False, name=None)))

'''
--- transform to final output
'''
# stock
update_stock_signal_final_api_data(conn)

# coin


# FIN
conn.close()