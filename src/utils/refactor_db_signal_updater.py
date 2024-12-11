from abc import ABC, abstractmethod
import pandas as pd
import psycopg2
from psycopg2 import OperationalError
from psycopg2.extras import execute_values
from config import *
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)8s | %(message)s',
    datefmt='%Y-%m-%d %H:%M' #datefmt='%Y-%m-%d %H:%M:%S'
)

class db_signal_updater(ABC): 
    '''object that 1) connect to db 2) ingest input data 3) insert output to db. 
    Template for stock and coin cointegration index calculation.
    '''
    def __init__(self, db_name, db_host, db_username, db_password):
        self.db_name = db_name
        self.db_host = db_host
        self.db_username = db_username
        self.db_password = db_password
        self.conn = None

    def connect(self):
        try:
            self.conn = psycopg2.connect(
                host=self.db_host,
                database=self.db_name,
                user=self.db_username,
                password=self.db_password)
            logging.info(f"Connected to {self.db_host} {self.db_name}!")
        except OperationalError as e:
            logging.error(f"Error connecting to database: {e}")
            self.conn = None

    def close(self):
        if self.conn:
            self.conn.close()
            logging.info("Database connection closed.")

    def pivot_price_data(self, df):
        df = df.drop_duplicates(subset=['date', 'symbol'])
        transformed_df = df.pivot(index='date', columns='symbol', values='close')
        transformed_df.fillna(-1, inplace=True)
        transformed_df.reset_index(inplace=True)
        return transformed_df
      
    @abstractmethod
    def fetch_input_data(self):
        pass
    
    @abstractmethod
    def _create_signal_data_table(self):
        pass
    
    @abstractmethod
    def insert_signal_data_table(self, signal_df):
        pass
 
class coin_stonewell_signal_updater(db_signal_updater):
    '''input data: top 20 coins
       calculation:    
    '''
    def fetch_input_data(self, top_n_tickers):
        query = f"""
        WITH top_tickers AS (
            SELECT DISTINCT a.symbol, market_cap
            FROM binance_coin_historical_price a
            join coin_overview b
            on a.symbol=b.symbol 
            WHERE market_cap IS NOT NULL
            ORDER BY market_cap DESC 
            LIMIT {top_n_tickers}
        )
        SELECT a.*
        FROM binance_coin_historical_price a 
        JOIN top_tickers b ON a.symbol = b.symbol
        WHERE date >= '2020-01-01'
        order by date
        """
        return pd.read_sql(query, self.conn)

    def _create_signal_data_table(self):
        cursor = self.conn.cursor()
        try:
            create_table_query = """
            CREATE TABLE IF NOT EXISTS coin_stonewell_signal (
                symbol VARCHAR(50) NOT NULL,
                close_above_sma BOOLEAN NOT NULL,
                close_above_sma_pct NUMERIC NOT NULL,
                rsi_above_sma BOOLEAN NOT NULL,
                short_vol_above_long BOOLEAN NOT NULL,
                death_cross BOOLEAN NOT NULL,
                last_updated TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (symbol)
            );
            """
            cursor.execute(create_table_query)
            self.conn.commit()
            logging.info("coin_stonewell_signal table created successfully.")
        except Exception as e:
            logging.error(f"Failed to create table: {str(e)}")
            self.conn.rollback()
        finally:
            cursor.close()
    
    def insert_signal_data_table(self, signal_df):
        self._create_signal_data_table()
        
        csv_as_tuple = list(signal_df.itertuples(index=False, name=None))
        cursor = self.conn.cursor()
        insert_query = """
        INSERT INTO coin_stonewell_signal (symbol, close_above_sma, close_above_sma_pct, rsi_above_sma, short_vol_above_long, death_cross, last_updated)
        VALUES %s
        ON CONFLICT (symbol) 
        DO UPDATE SET 
            close_above_sma = EXCLUDED.close_above_sma,
            close_above_sma_pct = EXCLUDED.close_above_sma_pct,
            rsi_above_sma = EXCLUDED.rsi_above_sma,
            short_vol_above_long = EXCLUDED.short_vol_above_long,
            death_cross = EXCLUDED.death_cross,
            last_updated = EXCLUDED.last_updated;
        """
        try:      
            chunk_size = 1000  # Increased chunk size for better performance
            for i in range(0, len(csv_as_tuple), chunk_size):
                execute_values(cursor, insert_query, csv_as_tuple[i:i+chunk_size])
            self.conn.commit()
            logging.info(f"Inserted/Updated {len(csv_as_tuple)} rows in coin_stonewell_signal table.")
        except Exception as e:
            logging.error(f"Failed to insert data: {e}")
            self.conn.rollback()
        finally:
            cursor.close()

class stock_coint_db_signal_updater(db_signal_updater):
    '''inherit db_signal_updater with custom stock output table creation and output data insertion'''
    def fetch_input_data(self, top_n_tickers):
        query = f"""
        WITH top_tickers AS (
            SELECT DISTINCT symbol, marketcapitalization
            FROM stock_overview 
            WHERE marketcapitalization IS NOT NULL
            ORDER BY marketcapitalization DESC 
            LIMIT {top_n_tickers}
        )
        SELECT a.*
        FROM stock_historical_price a 
        JOIN top_tickers b ON a.symbol = b.symbol
        WHERE date >= '2023-01-01'
        ORDER BY b.marketcapitalization DESC, a.date
        """
        return pd.read_sql(query, self.conn)

    def _create_output_data_table(self):
        cursor = self.conn.cursor()
        try:
            create_table_query = """
            CREATE TABLE IF NOT EXISTS stock_pairs_coint (
                date TIMESTAMPTZ NOT NULL,
                window_length INT NOT NULL,
                symbol1 VARCHAR(50) NOT NULL,
                symbol2 VARCHAR(50) NOT NULL,
                pvalue NUMERIC NOT NULL,
                PRIMARY KEY (date, window_length, symbol1, symbol2)
            );
            """
            cursor.execute(create_table_query)
            self.conn.commit()
            logging.info("stock_pairs_coint table created successfully.")
        except Exception as e:
            logging.error(f"Failed to create table: {str(e)}")
            self.conn.rollback()
        finally:
            cursor.close()
            
    def insert_output_data(self, output_df):
        self._create_output_data_table()
        
        csv_as_tuple = list(output_df.itertuples(index=False, name=None))
        cursor = self.conn.cursor()
        insert_query = """
        INSERT INTO stock_pairs_coint (date, window_length, symbol1, symbol2, pvalue)
        VALUES %s
        ON CONFLICT (date, window_length, symbol1, symbol2)
        DO UPDATE SET 
            pvalue = EXCLUDED.pvalue
        """
        try:      
            chunk_size = 1000  # Increased chunk size for better performance
            for i in range(0, len(csv_as_tuple), chunk_size):
                execute_values(cursor, insert_query, csv_as_tuple[i:i+chunk_size])
            self.conn.commit()
            logging.info(f"Inserted {len(csv_as_tuple)} rows into stock_pairs_coint table.")
        except Exception as e:
            logging.error(f"Failed to insert data: {e}")
            self.conn.rollback()
        finally:
            cursor.close()

    def _create_signal_data_table(self):
        cursor = self.conn.cursor()
        try:
            create_table_query = """
            CREATE TABLE IF NOT EXISTS stock_signal (
                symbol1 VARCHAR(50) NOT NULL,
                symbol2 VARCHAR(50) NOT NULL,
                window_length INT NOT NULL,
                most_recent_coint_pct NUMERIC NOT NULL, 
                recent_coint_pct NUMERIC NOT NULL, 
                hist_coint_pct NUMERIC NOT NULL, 
                r_squared NUMERIC NOT NULL, 
                ols_constant NUMERIC NOT NULL, 
                ols_coeff NUMERIC NOT NULL, 
                last_updated TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (symbol1, symbol2, window_length)
            );
            """
            cursor.execute(create_table_query)
            self.conn.commit()
            logging.info("stock_signal table created successfully.")
        except Exception as e:
            logging.error(f"Failed to create table: {str(e)}")
            self.conn.rollback()
        finally:
            cursor.close()
    
    def insert_signal_data_table(self, signal_df):
        self._create_signal_data_table()
        
        csv_as_tuple = list(signal_df.itertuples(index=False, name=None))
        cursor = self.conn.cursor()
        insert_query = """
        INSERT INTO stock_signal (symbol1, symbol2, window_length,most_recent_coint_pct, recent_coint_pct, hist_coint_pct, r_squared, ols_constant, ols_coeff, last_updated)
        VALUES %s
        ON CONFLICT (symbol1, symbol2, window_length) 
        DO UPDATE SET 
            most_recent_coint_pct = EXCLUDED.most_recent_coint_pct,
            recent_coint_pct = EXCLUDED.recent_coint_pct,
            hist_coint_pct = EXCLUDED.hist_coint_pct,
            r_squared = EXCLUDED.r_squared,
            ols_constant = EXCLUDED.ols_constant,
            ols_coeff = EXCLUDED.ols_coeff,
            last_updated = EXCLUDED.last_updated;
        """
        try:      
            chunk_size = 1000  # Increased chunk size for better performance
            for i in range(0, len(csv_as_tuple), chunk_size):
                execute_values(cursor, insert_query, csv_as_tuple[i:i+chunk_size])
            self.conn.commit()
            logging.info(f"Inserted/Updated {len(csv_as_tuple)} rows in stock_signal table.")
        except Exception as e:
            logging.error(f"Failed to insert data: {e}")
            self.conn.rollback()
        finally:
            cursor.close()
            
    def _create_api_output_table(self):
        cursor = self.conn.cursor()
        try:
            create_table_query = """
            CREATE TABLE IF NOT EXISTS stock_signal_api_output (
            symbol1 VARCHAR(50) NOT NULL,
            market_cap_1 BIGINT,
            pe_ratio_1 DECIMAL(10, 2),
            target_price_1 DECIMAL(10, 2),
            symbol2 VARCHAR(50) NOT NULL,
            market_cap_2 BIGINT,
            pe_ratio_2 DECIMAL(10, 2),
            target_price_2 DECIMAL(10, 2),
            most_recent_coint_pct NUMERIC,
            recent_coint_pct NUMERIC,
            hist_coint_pct NUMERIC,
            r_squared DECIMAL,
            ols_constant DECIMAL,
            ols_coeff DECIMAL,
            last_updated TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (symbol1, symbol2)
            );
            """
            cursor.execute(create_table_query)
            self.conn.commit()
            logging.info("stock_signal_api_output table created successfully.")
        except Exception as e:
            logging.error(f"Failed to create table: {str(e)}")
            self.conn.rollback()
        finally:
            cursor.close()

    def insert_api_output_data(self):
        self._create_api_output_table()
        cursor = self.conn.cursor()
        try:
            insert_data_query = """
            INSERT INTO stock_signal_api_output (symbol1, market_cap_1, pe_ratio_1, target_price_1, symbol2, market_cap_2, pe_ratio_2, target_price_2, most_recent_coint_pct, recent_coint_pct, hist_coint_pct, r_squared, ols_constant, ols_coeff, last_updated)
            SELECT 
                a.symbol1, 
                b.MarketCapitalization AS market_cap_1, 
                b.PERatio AS pe_ratio_1, 
                b.AnalystTargetPrice AS target_price_1,
                a.symbol2, 
                c.MarketCapitalization AS market_cap_2, 
                c.PERatio AS pe_ratio_2, 
                c.AnalystTargetPrice AS target_price_2,
                a.most_recent_coint_pct, 
                a.recent_coint_pct,
                a.hist_coint_pct,
                a.r_squared, 
                a.ols_constant, 
                a.ols_coeff, 
                a.last_updated
            FROM 
                stock_signal a 
            JOIN 
                stock_overview b ON a.symbol1 = b.symbol
            JOIN 
                stock_overview c ON a.symbol2 = c.symbol
            ORDER BY 
                a.most_recent_coint_pct DESC
            ON CONFLICT (symbol1, symbol2) 
            DO UPDATE SET 
            market_cap_1 = EXCLUDED.market_cap_1,
            pe_ratio_1 = EXCLUDED.pe_ratio_1,
            target_price_1 = EXCLUDED.target_price_1,
            market_cap_2 = EXCLUDED.market_cap_2,
            pe_ratio_2 = EXCLUDED.pe_ratio_2,
            target_price_2 = EXCLUDED.target_price_2,
            most_recent_coint_pct = EXCLUDED.most_recent_coint_pct,
            recent_coint_pct = EXCLUDED.recent_coint_pct,
            hist_coint_pct = EXCLUDED.hist_coint_pct,
            r_squared = EXCLUDED.r_squared,
            ols_constant = EXCLUDED.ols_constant,
            ols_coeff = EXCLUDED.ols_coeff,
            last_updated = EXCLUDED.last_updated;
            """
            cursor.execute(insert_data_query)
            self.conn.commit()
            logging.info("stock_signal_api_output data inserted/updated successfully.")
        except Exception as e:
            logging.error(f"Failed to insert/update data: {str(e)}")
            self.conn.rollback()
        finally:
            cursor.close()
            logging.info("API data update process completed.")
                     
class stock_coint_by_segment_db_signal_updater(stock_coint_db_signal_updater):
    '''variation of stock_coint_db_signal_updater, with new method of ingesting input data'''
    def fetch_input_data(self, top_n_tickers_by_sectors):
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
        return pd.read_sql(query, self.conn)
    
class coin_coint_db_signal_updater(db_signal_updater):
    '''inherit db_signal_updater with custom crypto output table creation and output data insertion'''
    def fetch_input_data(self, top_n_tickers):
        query = f"""
        with top_tickers as (
        select distinct symbol, market_cap
        from coin_overview 
        where market_cap is not null
        and symbol not in ('USDC', 'USDT')
        order by market_cap desc
        limit {top_n_tickers})
        
        select a.*
        from coin_historical_price a 
        join top_tickers b 
        on a.symbol=b.symbol
        where date >= '2022-01-01'
        order by market_cap desc, date
        """
        return pd.read_sql(query, self.conn)

    def _create_output_data_table(self):
        cursor = self.conn.cursor()
        try:
            create_table_query = """
            CREATE TABLE IF NOT EXISTS coin_pairs_coint (
                date TIMESTAMPTZ NOT NULL,
                window_length INT NOT NULL,
                symbol1 VARCHAR(50) NOT NULL,
                symbol2 VARCHAR(50) NOT NULL,
                pvalue NUMERIC NOT NULL,
                PRIMARY KEY (date, window_length, symbol1, symbol2)
            );
            """
            cursor.execute(create_table_query)
            self.conn.commit()
            logging.info("coin_pairs_coint table created successfully.")
        except Exception as e:
            logging.error(f"Failed to create table: {str(e)}")
            self.conn.rollback()
        finally:
            cursor.close()
            
    def insert_output_data(self, output_df):
        self._create_output_data_table()
        
        csv_as_tuple = list(output_df.itertuples(index=False, name=None))
        cursor = self.conn.cursor()
        insert_query = """
        INSERT INTO coin_pairs_coint (date, window_length, symbol1, symbol2, pvalue)
        VALUES %s
        ON CONFLICT (date, window_length, symbol1, symbol2)
        DO UPDATE SET 
            pvalue = EXCLUDED.pvalue
        """
        try:      
            chunk_size = 1000  # Increased chunk size for better performance
            for i in range(0, len(csv_as_tuple), chunk_size):
                execute_values(cursor, insert_query, csv_as_tuple[i:i+chunk_size])
            self.conn.commit()
            logging.info(f"Inserted {len(csv_as_tuple)} rows into coin_pairs_coint table.")
        except Exception as e:
            logging.error(f"Failed to insert data: {e}")
            self.conn.rollback()
        finally:
            cursor.close()

    def _create_signal_data_table(self):
        cursor = self.conn.cursor()
        try:
            create_table_query = """
            CREATE TABLE IF NOT EXISTS coin_signal (
                symbol1 VARCHAR(50) NOT NULL,
                symbol2 VARCHAR(50) NOT NULL,
                window_length INT NOT NULL,
                most_recent_coint_pct NUMERIC NOT NULL, 
                recent_coint_pct NUMERIC NOT NULL, 
                hist_coint_pct NUMERIC NOT NULL, 
                r_squared NUMERIC NOT NULL, 
                ols_constant NUMERIC NOT NULL, 
                ols_coeff NUMERIC NOT NULL, 
                last_updated TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (symbol1, symbol2, window_length)
            );
            """
            cursor.execute(create_table_query)
            self.conn.commit()
            print("coin_signal table created successfully.")
        except Exception as e:
            print(f"Failed to create table: {str(e)}")
            self.conn.rollback()
        finally:
            cursor.close()
    
    def insert_signal_data_table(self, signal_df):
        self._create_signal_data_table()
        
        csv_as_tuple = list(signal_df.itertuples(index=False, name=None))
        cursor = self.conn.cursor()
        insert_query = """
        INSERT INTO coin_signal (symbol1, symbol2, window_length, most_recent_coint_pct, recent_coint_pct, hist_coint_pct, r_squared, ols_constant, ols_coeff, last_updated)
        VALUES %s
        ON CONFLICT (symbol1, symbol2, window_length) 
        DO UPDATE SET 
            most_recent_coint_pct = EXCLUDED.most_recent_coint_pct,
            recent_coint_pct = EXCLUDED.recent_coint_pct,
            hist_coint_pct = EXCLUDED.hist_coint_pct,
            r_squared = EXCLUDED.r_squared,
            ols_constant = EXCLUDED.ols_constant,
            ols_coeff = EXCLUDED.ols_coeff,
            last_updated = EXCLUDED.last_updated;
        """
        try:      
            chunk_size = 1000  # Increased chunk size for better performance
            for i in range(0, len(csv_as_tuple), chunk_size):
                execute_values(cursor, insert_query, csv_as_tuple[i:i+chunk_size])
            self.conn.commit()
            print(f"Inserted/Updated {len(csv_as_tuple)} rows in coin_signal table.")
        except Exception as e:
            print(f"Failed to insert data: {e}")
            self.conn.rollback()
        finally:
            cursor.close()
            
    def _create_api_output_table(self):
        cursor = self.conn.cursor()
        try:
            create_table_query = """
            CREATE TABLE IF NOT EXISTS coin_signal_api_output (
            symbol1 VARCHAR(50) NOT NULL,
            name1 VARCHAR(50) NOT NULL,
            market_cap_1 BIGINT,
            symbol2 VARCHAR(50) NOT NULL,
            name2 VARCHAR(50) NOT NULL,
            market_cap_2 BIGINT,
            most_recent_coint_pct NUMERIC,
            recent_coint_pct NUMERIC,
            hist_coint_pct NUMERIC,
            r_squared DECIMAL,
            ols_constant DECIMAL,
            ols_coeff DECIMAL,
            last_updated TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (symbol1, name1, symbol2, name2)
            );
            """
            cursor.execute(create_table_query)
            self.conn.commit()
            print("coin_signal_api_output table created successfully.")
        except Exception as e:
            print(f"Failed to create table: {str(e)}")
            self.conn.rollback()
        finally:
            cursor.close()

    def insert_api_output_data(self):
        self._create_api_output_table()
        cursor = self.conn.cursor()
        try:
            insert_data_query = """
        INSERT INTO coin_signal_api_output (symbol1, name1, market_cap_1, symbol2, name2, market_cap_2, most_recent_coint_pct, recent_coint_pct, hist_coint_pct, r_squared, ols_constant, ols_coeff, last_updated)
        SELECT distinct
            a.symbol1, 
            b.name as name1,
            b.market_cap AS market_cap_1, 
            a.symbol2, 
            c.name as name2,
            c.market_cap AS market_cap_2, 
            a.most_recent_coint_pct, 
            a.recent_coint_pct,
            a.hist_coint_pct,
            a.r_squared, 
            a.ols_constant, 
            a.ols_coeff, 
            a.last_updated
        FROM 
            coin_signal a 
        JOIN 
            coin_overview b ON a.symbol1 = b.symbol
        JOIN 
            coin_overview c ON a.symbol2 = c.symbol
        ORDER BY 
            a.most_recent_coint_pct DESC
        ON CONFLICT (symbol1, name1, symbol2, name2)
        DO UPDATE SET 
        market_cap_1 = EXCLUDED.market_cap_1,
        market_cap_2 = EXCLUDED.market_cap_2,
        most_recent_coint_pct = EXCLUDED.most_recent_coint_pct,
        recent_coint_pct = EXCLUDED.recent_coint_pct,
        hist_coint_pct = EXCLUDED.hist_coint_pct,
        r_squared = EXCLUDED.r_squared,
        ols_constant = EXCLUDED.ols_constant,
        ols_coeff = EXCLUDED.ols_coeff,
        last_updated = EXCLUDED.last_updated;
        """
            cursor.execute(insert_data_query)
            self.conn.commit()
            print("coin_signal_api_output data inserted/updated successfully.")
        except Exception as e:
            print(f"Failed to insert/update data: {str(e)}")
            self.conn.rollback()
        finally:
            cursor.close()
            print("API data update process completed.")
                     