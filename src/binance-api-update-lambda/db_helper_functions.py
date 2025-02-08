from abc import ABC, abstractmethod
import os
import json
import pandas as pd
import psycopg2
from psycopg2 import OperationalError
from psycopg2.extras import execute_values
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)8s | %(message)s',
    datefmt='%Y-%m-%d %H:%M' #datefmt='%Y-%m-%d %H:%M:%S'
)

def convert_to_float(value):
    if value is None or value == "" or value == "None" or value == "-":
        return None
    try:
        return float(value)
    except (ValueError, TypeError) as e:
        logging.error(f"Error converting to float: {e}")
        return None

def convert_to_int(value):
    if value is None or value == "" or value == "None" or value == "-":
        return None
    try:
        return int(value)
    except (ValueError, TypeError) as e:
        logging.error(f"Error converting to int: {e}")
        return None
    
def convert_to_date(value):
    if value in ("None", '-'):
        return None
    try:
        return datetime.strptime(value, '%Y-%m-%d').date()
    except ValueError as e:
        logging.error(f"Error converting to date: {e}")
        return None

def convert_to_datetime(date_str):
    try:
        return datetime.fromisoformat(date_str.replace("Z", ""))
    except ValueError as e:
        logging.error(f"Error converting to datetime: {e}")
        return None
    
def truncate_string(value, length):
    try:
        if value and len(value) > length:
            logging.warning(f'Truncating {value} at {length}')
        return value[:length] if value and len(value) > length else value
    except Exception as e:
        logging.error(f"Error truncating string: {e}")
        return None


class db_refresher(ABC): 
    '''object that 1) connect to db 2) transform and insert json data depends on source.
       template for coin_gecko_db and avan_stock_db'''
    def __init__(self, db_name, db_host, db_username, db_password, table_name):
        self.db_name = db_name
        self.db_host = db_host
        self.db_username = db_username
        self.db_password = db_password
        self.conn = None
        self.table_name = table_name  
        self.table_creation_script = None  # This will be set in child classes
        self.data_insertion_script = None  # This will be set in child classes
         
    def connect(self):
        try:
            self.conn = psycopg2.connect(
                host=self.db_host,
                database=self.db_name,
                user=self.db_username,
                password=self.db_password)
            print(f"Connected to {self.db_host} {self.db_name}!")
        except OperationalError as e:
            print(f"Error connecting to database: {e}")
            self.conn = None

    def close(self):
        if self.conn:
            self.conn.close()
            print("Database connection closed.")
      
    def create_table(self):
        cursor = self.conn.cursor()
        try:
            cursor.execute(self.table_creation_script)
            self.conn.commit()
            logging.info(f"{self.table_name} created successfully.")
        except Exception as e:
            logging.error(f"Failed to create table: {str(e)}")
            self.conn.rollback()
        finally:
            cursor.close()
    
    def delete_table(self):
        cursor = self.conn.cursor()
        try:
            cursor.execute(f"DROP TABLE IF EXISTS {self.table_name}")
            self.conn.commit()
            logging.info(f"{self.table_name} deleted successfully.")
        except Exception as e:
            logging.error(f"Failed to delete table: {str(e)}")
            self.conn.rollback()
        finally:
            cursor.close()
    
    def _data_transformation(self, file_path):
        pass
    
    def insert_data(self, file_path):
        time_series_data = self._data_transformation(file_path)
        cursor = self.conn.cursor()
        try:
            execute_values(cursor, self.data_insertion_script, time_series_data)
            self.conn.commit()
            logging.debug(f"Inserted into {self.table_name} from {file_path}")
        except Exception as e:
            logging.error(f"Failed to insert data from {file_path}: {e}")
            self.conn.rollback()
        finally:
            cursor.close()

    def get_data(self, columns=["*"], where_clause=None):
        """Retrieve data from the table
        
        Args:
            columns (list): List of column names to select, defaults to ["*"] for all columns
            where_clause (str): Optional WHERE clause for filtering
            
        Returns:
            pandas.DataFrame: Retrieved data as a DataFrame
        """
        cursor = self.conn.cursor()
        try:
            columns_str = ", ".join(columns)
            query = f"SELECT distinct {columns_str} FROM {self.table_name}"
            if where_clause:
                query += f" WHERE {where_clause}"
                
            cursor.execute(query)
            data = cursor.fetchall()
            
            # Get column names from cursor description
            column_names = [desc[0] for desc in cursor.description]
            
            # Convert to pandas DataFrame
            df = pd.DataFrame(data, columns=column_names)
            return df
            
        except Exception as e:
            logging.error(f"Failed to retrieve data: {str(e)}")
            return None
        finally:
            cursor.close()
            
    def _get_max_date_for_coint_pair(self):
        """
        hacky way to get the last updated date for coint pairs
        """
        cursor = self.conn.cursor()
        try:
            query = f'''
            SELECT symbol_one, symbol_two, MAX(date) as max_date, window_size
            FROM {self.table_name}
            GROUP BY symbol_one, symbol_two, window_size
            '''
                
            cursor.execute(query)
            data = cursor.fetchall()
            
            # Get column names from cursor description
            column_names = [desc[0] for desc in cursor.description]
            
            # Convert to pandas DataFrame
            df = pd.DataFrame(data, columns=column_names)
            return df
            
        except Exception as e:
            logging.error(f"Failed to retrieve data: {str(e)}")
            return None
        finally:
            cursor.close()
    
class binance_OHLC_db_refresher(db_refresher):
    '''handle all data insertion from OHLC data via binance api'''
    def __init__(self, *args):
        super().__init__(*args)
        
        self.table_creation_script = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            symbol VARCHAR(20) NOT NULL,
            date DATE NOT NULL,
            open DOUBLE PRECISION NOT NULL,
            high DOUBLE PRECISION NOT NULL,
            low DOUBLE PRECISION NOT NULL,
            close DOUBLE PRECISION NOT NULL,
            volume DOUBLE PRECISION NOT NULL,
            close_time DATE NOT NULL,
            quote_volume DOUBLE PRECISION NOT NULL,
            trades INTEGER NOT NULL,
            taker_base_volume DOUBLE PRECISION NOT NULL,
            taker_quote_volume DOUBLE PRECISION NOT NULL,
            PRIMARY KEY (symbol, date)
        );
        """
        
        self.data_insertion_script = f"""
        INSERT INTO {self.table_name} (
            symbol, date, open, high, low, close, volume,
            close_time, quote_volume, trades, taker_base_volume, taker_quote_volume
        )
        VALUES %s
        ON CONFLICT (symbol, date)
        DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume,
            close_time = EXCLUDED.close_time,
            quote_volume = EXCLUDED.quote_volume,
            trades = EXCLUDED.trades,
            taker_base_volume = EXCLUDED.taker_base_volume,
            taker_quote_volume = EXCLUDED.taker_quote_volume;
        """
        
    def _data_transformation(self, file_path):
        try:
            with open(file_path, 'r') as file:
                data = json.load(file)
            symbol = os.path.splitext(os.path.basename(file_path))[0]
            outputs = []
            seen_dates = set()
            
            for entry in data:
                timestamp_ms = entry[0]
                date = datetime.fromtimestamp(timestamp_ms/1000).strftime('%Y-%m-%d')
                open_price = float(entry[1])
                high = float(entry[2])
                low = float(entry[3])
                close = float(entry[4])
                volume = float(entry[5])      # Trading volume in base asset
                close_time = datetime.fromtimestamp(entry[6]/1000).strftime('%Y-%m-%d')
                quote_volume = float(entry[7])         # Volume in USDT
                trades = int(entry[8])                 # Number of trades
                taker_base_volume = float(entry[9])    # Taker volume in base asset
                taker_quote_volume = float(entry[10])  # Taker volume in quote asset
                               
                if date not in seen_dates:
                    outputs.append([
                        symbol, date, open_price, high, low, close, volume,
                        close_time, quote_volume, trades, taker_base_volume,
                        taker_quote_volume
                    ])
                    seen_dates.add(date)  
            return outputs     
        except Exception as e:
            logging.debug(f"Data transformation failed for {symbol}: {e}")
            return None

class binance_cointegration_db_refresher(db_refresher):
    '''handle all data insertion from OHLC data via binance api'''
    def __init__(self, *args):
        super().__init__(*args)
        
        self.table_creation_script = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            symbol_one VARCHAR(20) NOT NULL,
            symbol_two VARCHAR(20) NOT NULL,
            date DATE NOT NULL,
            window_size INTEGER NOT NULL,
            coint_p_value DOUBLE PRECISION NOT NULL,
            PRIMARY KEY (symbol_one, symbol_two, date)
        );
        """
        
        self.data_insertion_script = f"""
        INSERT INTO {self.table_name} (
            symbol_one, symbol_two, date, window_size, coint_p_value
        )
        VALUES %s
        ON CONFLICT (symbol_one, symbol_two, date)
        DO UPDATE SET
            window_size = EXCLUDED.window_size,
            coint_p_value = EXCLUDED.coint_p_value;
        """
        
    def _data_transformation(self, pd_df):
        try:
            outputs = []
            seen_pairs = set()
            
            for _, row in pd_df.iterrows():
                symbol1 = row['symbol1']
                symbol2 = row['symbol2']
                date = pd.to_datetime(row['date']).strftime('%Y-%m-%d')
                window_size = int(row['window_size'])
                p_value = float(row['p_value'])
                
                pair_key = (symbol1, symbol2, date)
                if pair_key not in seen_pairs:
                    outputs.append([
                        symbol1, symbol2, date, window_size, p_value
                    ])
                    seen_pairs.add(pair_key)
                    
            return outputs
        except Exception as e:
            logging.debug(f"Data transformation failed: {e}")
            return None
        
class binance_performance_db_refresher(db_refresher):
    '''handle all data insertion for performance metrics'''
    def __init__(self, *args):
        super().__init__(*args)
        
        self.table_creation_script = f""" 
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            symbol VARCHAR(20) NOT NULL,
            latest_date DATE NOT NULL,
            latest_close DOUBLE PRECISION NOT NULL,
            close_7d_ago DOUBLE PRECISION,
            close_30d_ago DOUBLE PRECISION,
            close_60d_ago DOUBLE PRECISION,
            close_90d_ago DOUBLE PRECISION,
            close_180d_ago DOUBLE PRECISION,
            avg_volume_14d DOUBLE PRECISION,
            pct_change_7d DOUBLE PRECISION,
            pct_change_30d DOUBLE PRECISION,
            pct_change_60d DOUBLE PRECISION,
            pct_change_90d DOUBLE PRECISION,
            pct_change_180d DOUBLE PRECISION,
            pct_change_365d DOUBLE PRECISION,
            pct_change_altseason_1 DOUBLE PRECISION,
            pct_change_altseason_2 DOUBLE PRECISION,
            pct_change_altseason_3 DOUBLE PRECISION,
            PRIMARY KEY (symbol)
        );
        """
        
        self.data_insertion_script = f"""
        INSERT INTO {self.table_name} (
            symbol, latest_date, latest_close, close_7d_ago, close_30d_ago, close_60d_ago, close_90d_ago, close_180d_ago, avg_volume_14d, pct_change_7d, pct_change_30d, pct_change_60d, pct_change_90d, pct_change_180d, pct_change_365d, pct_change_altseason_1, pct_change_altseason_2, pct_change_altseason_3
        )
        WITH t1 AS (
                SELECT symbol, MAX(date) AS latest_date
                FROM binance_market_data  
                GROUP BY symbol
            ),
            latest_date AS (
                SELECT MAX(latest_date) as max_date
                FROM t1
            ),
            t2 AS (
                SELECT symbol, date, close, quote_volume,
                    LAG(close, 7) OVER (PARTITION BY symbol ORDER BY date) AS close_7d_ago,
                    LAG(close, 30) OVER (PARTITION BY symbol ORDER BY date) AS close_30d_ago,
                    LAG(close, 60) OVER (PARTITION BY symbol ORDER BY date) AS close_60d_ago,
                    LAG(close, 90) OVER (PARTITION BY symbol ORDER BY date) AS close_90d_ago,
                    LAG(close, 180) OVER (PARTITION BY symbol ORDER BY date) AS close_180d_ago,
                    LAG(close, 365) OVER (PARTITION BY symbol ORDER BY date) AS close_365d_ago,
                    AVG(quote_volume) OVER (PARTITION BY symbol ORDER BY date
                                            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS avg_volume_14d
                FROM binance_market_data
            ),
            custom_altseason_1 AS (
                SELECT DISTINCT symbol, close AS altseason_1_start_close
                FROM binance_market_data
                WHERE date = '2019-12-23'
            ),
            custom_altseason_1_end AS (
                SELECT DISTINCT symbol, close AS altseason_1_end_close
                FROM binance_market_data
                WHERE date = '2020-08-17'
            ),
            custom_altseason_2 AS (
                SELECT DISTINCT symbol, close AS altseason_2_start_close
                FROM binance_market_data
                WHERE date = '2021-01-04'
            ),
            custom_altseason_2_end AS (
                SELECT DISTINCT symbol, close AS altseason_2_end_close
                FROM binance_market_data
                WHERE date = '2022-01-10'
            ),
            custom_altseason_3 AS (
                SELECT DISTINCT symbol, close AS altseason_3_start_close
                FROM binance_market_data
                WHERE date = '2023-10-30'
            ),
            custom_altseason_3_end AS (
                SELECT DISTINCT symbol, close AS altseason_3_end_close
                FROM binance_market_data
                WHERE date = '2024-03-25'
            )
            SELECT 
                t2.symbol, 
                t2.date AS latest_date, 
                t2.close AS latest_close, 
                t2.close_7d_ago, 
                t2.close_30d_ago, 
                t2.close_60d_ago, 
                t2.close_90d_ago, 
                t2.close_180d_ago,
                t2.avg_volume_14d,
                (t2.close - t2.close_7d_ago) / t2.close_7d_ago AS pct_change_7d,
                (t2.close - t2.close_30d_ago) / t2.close_30d_ago AS pct_change_30d,
                (t2.close - t2.close_60d_ago) / t2.close_60d_ago AS pct_change_60d,
                (t2.close - t2.close_90d_ago) / t2.close_90d_ago AS pct_change_90d,
                (t2.close - t2.close_180d_ago) / t2.close_180d_ago AS pct_change_180d,
                (t2.close - t2.close_365d_ago) / t2.close_365d_ago AS pct_change_365d,
                (custom_altseason_1_end.altseason_1_end_close - custom_altseason_1.altseason_1_start_close)
                / custom_altseason_1.altseason_1_start_close AS pct_change_altseason_1,
                (custom_altseason_2_end.altseason_2_end_close - custom_altseason_2.altseason_2_start_close)
                / custom_altseason_2.altseason_2_start_close AS pct_change_altseason_2,
                (custom_altseason_3_end.altseason_3_end_close - custom_altseason_3.altseason_3_start_close)
                / custom_altseason_3.altseason_3_start_close AS pct_change_altseason_3
            FROM t2
            JOIN t1 ON t2.symbol = t1.symbol AND t2.date = t1.latest_date
            JOIN latest_date ON t1.latest_date = latest_date.max_date
            LEFT JOIN custom_altseason_1 ON t2.symbol = custom_altseason_1.symbol
            LEFT JOIN custom_altseason_1_end ON t2.symbol = custom_altseason_1_end.symbol
            LEFT JOIN custom_altseason_2 ON t2.symbol = custom_altseason_2.symbol
            LEFT JOIN custom_altseason_2_end ON t2.symbol = custom_altseason_2_end.symbol
            LEFT JOIN custom_altseason_3 ON t2.symbol = custom_altseason_3.symbol
            LEFT JOIN custom_altseason_3_end ON t2.symbol = custom_altseason_3_end.symbol
        ON CONFLICT (symbol)
        DO NOTHING
        """
        
    def insert_data(self):
        cursor = self.conn.cursor()
        try:
            cursor.execute(self.data_insertion_script)
            self.conn.commit()
            logging.info(f"Successfully executed query on {self.table_name}")
        except Exception as e:
            logging.error(f"Failed to execute query: {e}")
            self.conn.rollback()
        finally:
            cursor.close()