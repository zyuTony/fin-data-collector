from config import *
from utils.db_utils import *
from utils.avan_utils import *
from financial_database.backend.archives.calc_utils import *
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
top_n_tickers_by_sectors = 30
checkpoint_file_path = CHECKPOINT_JSON_PATH+'/calc_pipeline.json'
coint_csv_path = COINT_CSV_PATH+'/calc_pipeline_coint_by_segment.csv'
signal_csv_path = SIGNAL_CSV_PATH+'/calc_pipeline_signal_by_segment.csv'

conn = connect_to_db(DB_NAME, DB_HOST, DB_USERNAME, DB_PASSWORD)
query = f"""
WITH ranked_stocks AS (
SELECT symbol, sector, marketcapitalization,
ROW_NUMBER() OVER (PARTITION BY sector ORDER BY marketcapitalization DESC) AS rn
FROM stock_overview
WHERE marketcapitalization IS NOT NULL),
top_stocks_by_sector as (
SELECT symbol, sector, marketcapitalization
FROM ranked_stocks
WHERE rn <= {top_n_tickers_by_sectors}
ORDER BY sector, marketcapitalization DESC)

select b.sector, a.*
from stock_historical_price a 
join top_stocks_by_sector b 
on a.symbol=b.symbol
where date >= '2023-01-01'
order by marketcapitalization desc, date
"""
df = pd.read_sql(query, conn)
conn.close()

df = df.drop_duplicates(subset=['date', 'symbol'])
price_df = df.pivot(index='date', columns='symbol', values='close')
price_df.fillna(-1, inplace=True)
price_df.reset_index(inplace=True)

all_results = pd.DataFrame()
sector_groups = df.groupby('sector')
for sector, group in sector_groups:
    sector_price_df = group.pivot(index='date', columns='symbol', values='close')
    sector_price_df.fillna(-1, inplace=True)
    sector_price_df.reset_index(inplace=True)
    sector_coint_csv_path = f"{os.path.splitext(coint_csv_path)[0]}_{sector}.csv"
    print(f'working on {sector} sector')

    sector_results = save_multi_pairs_rolling_coint(sector_price_df, None, checkpoint_file_path, sector_coint_csv_path)
    sector_results.set_index('date', inplace=True)
    all_results = pd.concat([all_results, sector_results], axis=1, join='outer')

all_results.reset_index(inplace=True)
all_results.to_csv(coint_csv_path, index=False)

# insert coint to db
coint_df = pd.read_csv(coint_csv_path)
coint_df.columns = coint_df.columns.str.replace('_p_val$', '', regex=True)
coint_df_melted = pd.melt(coint_df, id_vars=['date'], var_name='pair_name', value_name='value')
coint_df_melted[['symbol1', 'symbol2']] = coint_df_melted['pair_name'].str.split('_', expand=True)
coint_df_melted = coint_df_melted.drop(columns=['pair_name'])
coint_df_melted['window_length'] = ROLLING_COINT_WINDOW
coint_df_melted = coint_df_melted[['date', 'window_length', 'symbol1', 'symbol2', 'value']]

conn = connect_to_db(DB_NAME, DB_HOST, DB_USERNAME, DB_PASSWORD)
create_stock_pair_coint_table(conn)
insert_stock_pair_coint_table(conn, list(coint_df_melted.itertuples(index=False, name=None)))

# get signal 
signal_df = get_signal(coint_df, price_df)
signal_df.to_csv(signal_csv_path, index=False)

# insert to db
create_stock_signal_table(conn)
insert_stock_signal_table(conn, list(signal_df.itertuples(index=False, name=None)))

# update api data after calculation
update_stock_signal_final_api_data(conn)
conn.close()

 





 