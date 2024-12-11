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

'''
Stock Overview Refresh Pipeline
Cadence: AUTOMATIC WEEKLY
  1. Download stock overview json from AVAN
  2. Insert overview data into DB
'''
# download json
api_getter = avan_stock_economic_api_getter(api_key=avan_api_key,
                                            data_save_path=AVAN_ECONOMIC_JSON_PATH)
# api_getter.download_data()
data = api_getter.aggregate_economic_data()
 
# insert to db
db = avan_stock_economic_db_refresher(DB_NAME, DB_HOST, DB_USERNAME, DB_PASSWORD, "stock_economics")
db.connect()
db.delete_table()
db.create_table()
db.insert_data(os.path.join(AVAN_ECONOMIC_JSON_PATH, 'aggregated_economic_data.json'))
db.close() 


# # download json
# api_getter = avan_stock_cash_flow_api_getter(api_key=avan_api_key,
#                                             data_save_path=AVAN_CASH_FLOW_JSON_PATH,
#                                             start_date=None,# not applicable
#                                             end_date=None,# not applicable
#                                             additional_tickers=[])
# api_getter.download_data()

# # insert to db
# db = avan_stock_cash_flow_db_refresher(DB_NAME, DB_HOST, DB_USERNAME, DB_PASSWORD, "stock_cashflow")
# db.connect()
# db.delete_table()
# db.create_table()
# for filename in os.listdir(AVAN_CASH_FLOW_JSON_PATH):
#     if filename.endswith('.json'):
#         file_path = os.path.join(AVAN_CASH_FLOW_JSON_PATH, filename)
#         db.insert_data(file_path)
# db.close() 

# # download json
# api_getter = avan_stock_income_statement_api_getter(api_key=avan_api_key,
#                                             data_save_path=AVAN_INCOME_STATEMENT_JSON_PATH,
#                                             start_date=None,# not applicable
#                                             end_date=None,# not applicable
#                                             additional_tickers=[])
# api_getter.download_data()

# # insert to db
# db = avan_stock_income_statement_db_refresher(DB_NAME, DB_HOST, DB_USERNAME, DB_PASSWORD, "stock_income_statement")
# db.connect()
# db.delete_table()
# db.create_table()
# for filename in os.listdir(AVAN_INCOME_STATEMENT_JSON_PATH):
#     if filename.endswith('.json'):
#         file_path = os.path.join(AVAN_INCOME_STATEMENT_JSON_PATH, filename)
#         db.insert_data(file_path)
# db.close() 

# # download json
# api_getter = avan_stock_balance_sheet_api_getter(api_key=avan_api_key,
#                                             data_save_path=AVAN_BALANCE_SHEET_JSON_PATH,
#                                             start_date=None,# not applicable
#                                             end_date=None,# not applicable
#                                             additional_tickers=[])
# api_getter.download_data()

# # insert to db
# db = avan_stock_balance_sheet_db_refresher(DB_NAME, DB_HOST, DB_USERNAME, DB_PASSWORD, "stock_balance_sheet")
# db.connect()
# db.delete_table()
# db.create_table()
# for filename in os.listdir(AVAN_BALANCE_SHEET_JSON_PATH):
#     if filename.endswith('.json'):
#         file_path = os.path.join(AVAN_BALANCE_SHEET_JSON_PATH, filename)
#         db.insert_data(file_path)
# db.close() 