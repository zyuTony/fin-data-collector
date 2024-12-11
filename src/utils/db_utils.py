import os
import pandas as pd
import psycopg2
from psycopg2 import OperationalError
from psycopg2.extras import execute_values
from datetime import datetime 
import json

def connect_to_db(DB_NAME, DB_HOST, DB_USERNAME, DB_PASSWORD):
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USERNAME,
            password=DB_PASSWORD)
        print(f"Connected to {DB_HOST} {DB_NAME}!")
        return conn
    except OperationalError as e:
        print(f"{e}")
        return None

# general functions
def convert_to_float(value):
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None

def convert_to_int(value):
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None
    
def convert_to_date(value):
    return datetime.strptime(value, '%Y-%m-%d').date() if value not in ("None", '-') else None

def convert_to_datetime(date_str):
    try:
        return datetime.fromisoformat(date_str.replace("Z", ""))
    except ValueError:
        return None
    
def truncate_string(value, length):
    if value and len(value) > length:
        print(f'trunc {value} at {length}')
    return value[:length] if value and len(value) > length else value


# Table Creations
def create_stock_historical_price_table(conn):
    cursor = conn.cursor()
    try:
        create_table_query = """
        CREATE TABLE IF NOT EXISTS stock_historical_price (
            symbol VARCHAR(10) NOT NULL,
            date TIMESTAMPTZ NOT NULL,
            open NUMERIC NOT NULL,
            high NUMERIC NOT NULL,
            low NUMERIC NOT NULL,
            close NUMERIC NOT NULL,
            volume BIGINT NOT NULL,
            PRIMARY KEY (symbol, date)
        );
        """
        cursor.execute(create_table_query)
        conn.commit()
        print(f"stock_historical_price created successfully.")
    except Exception as e:
        print(f"Failed to create table: {str(e)}")
        conn.rollback()
    finally:
        cursor.close()

def create_stock_overview_table(conn):
    cursor = conn.cursor()
    try:
        create_table_query = """
            CREATE TABLE IF NOT EXISTS stock_overview (
            Symbol VARCHAR(10) PRIMARY KEY,
            AssetType VARCHAR(50),
            Name VARCHAR(255),
            Description TEXT,
            CIK VARCHAR(10),
            Exchange VARCHAR(10),
            Currency VARCHAR(10),
            Country VARCHAR(50),
            Sector VARCHAR(50),
            Industry VARCHAR(255),
            Address VARCHAR(255),
            FiscalYearEnd VARCHAR(20),
            LatestQuarter DATE,
            MarketCapitalization BIGINT,
            EBITDA BIGINT,
            PERatio DECIMAL(10, 2),
            PEGRatio DECIMAL(10, 3),
            BookValue DECIMAL(10, 2),
            DividendPerShare DECIMAL(10, 2),
            DividendYield DECIMAL(10, 4),
            EPS DECIMAL(10, 2),
            RevenuePerShareTTM DECIMAL(10, 2),
            ProfitMargin DECIMAL(10, 3),
            OperatingMarginTTM DECIMAL(10, 3),
            ReturnOnAssetsTTM DECIMAL(10, 3),
            ReturnOnEquityTTM DECIMAL(10, 3),
            RevenueTTM BIGINT,
            GrossProfitTTM BIGINT,
            DilutedEPSTTM DECIMAL(10, 2),
            QuarterlyEarningsGrowthYOY DECIMAL(10, 3),
            QuarterlyRevenueGrowthYOY DECIMAL(10, 3),
            AnalystTargetPrice DECIMAL(10, 2),
            AnalystRatingStrongBuy INT,
            AnalystRatingBuy INT,
            AnalystRatingHold INT,
            AnalystRatingSell INT,
            AnalystRatingStrongSell INT,
            TrailingPE DECIMAL(10, 2),
            ForwardPE DECIMAL(10, 2),
            PriceToSalesRatioTTM DECIMAL(10, 3),
            PriceToBookRatio DECIMAL(10, 2),
            EVToRevenue DECIMAL(10, 3),
            EVToEBITDA DECIMAL(10, 2),
            Beta DECIMAL(10, 3),
            High52Week DECIMAL(10, 2),
            Low52Week DECIMAL(10, 2),
            MovingAverage50Day DECIMAL(10, 2),
            MovingAverage200Day DECIMAL(10, 2),
            SharesOutstanding BIGINT,
            DividendDate DATE,
            ExDividendDate DATE
        );
        """
        cursor.execute(create_table_query)
        conn.commit()
        print(f"stock_overview created successfully.")
    except Exception as e:
        print(f"Failed to create table: {str(e)}")
        conn.rollback()
    finally:
        cursor.close()

def create_coin_historical_price_table(conn):
    cursor = conn.cursor()
    try:
        create_table_query = """
        CREATE TABLE IF NOT EXISTS coin_historical_price (
        symbol VARCHAR(20) NOT NULL,
        date TIMESTAMPTZ NOT NULL,
        open NUMERIC NOT NULL,
        high NUMERIC NOT NULL,
        low NUMERIC NOT NULL,
        close NUMERIC NOT NULL,
        PRIMARY KEY (symbol, date)
        );
        """
        cursor.execute(create_table_query)
        conn.commit()
        print(f"coin_historical_price created successfully.")
    except Exception as e:
        print(f"Failed to create table: {str(e)}")
        conn.rollback()
    finally:
        cursor.close()

def create_coin_hourly_historical_price_table(conn):
    cursor = conn.cursor()
    try:
        create_table_query = """
        CREATE TABLE IF NOT EXISTS coin_hourly_historical_price (
        symbol VARCHAR(20) NOT NULL,
        date TIMESTAMPTZ NOT NULL,
        open NUMERIC NOT NULL,
        high NUMERIC NOT NULL,
        low NUMERIC NOT NULL,
        close NUMERIC NOT NULL,
        PRIMARY KEY (symbol, date)
        );
        """
        cursor.execute(create_table_query)
        conn.commit()
        print(f"coin_hourly_historical_price created successfully.")
    except Exception as e:
        print(f"Failed to create table: {str(e)}")
        conn.rollback()
    finally:
        cursor.close()
        
def create_coin_overview_table(conn):
    cursor = conn.cursor()
    try:
        create_table_query = """
            CREATE TABLE IF NOT EXISTS coin_overview (
            symbol VARCHAR(10) NOT NULL,
            name VARCHAR(255),
            current_price DECIMAL(20, 8),  
            market_cap BIGINT,
            market_cap_rank INT,
            fully_diluted_valuation BIGINT,
            total_volume BIGINT,
            high_24h DECIMAL(20, 8),  
            low_24h DECIMAL(20, 8),  
            price_change_24h DECIMAL(20, 8),  
            price_change_percentage_24h DECIMAL(10, 6),  
            market_cap_change_24h BIGINT,
            market_cap_change_percentage_24h DECIMAL(10, 6),  
            circulating_supply DECIMAL(30, 10),  
            total_supply DECIMAL(30, 10),  
            max_supply DECIMAL(30, 10),  
            ath DECIMAL(20, 8),  
            ath_date TIMESTAMP,
            atl DECIMAL(20, 8),  
            atl_date TIMESTAMP,
            last_updated TIMESTAMP,
            PRIMARY KEY (symbol, name)
        );
        """
        cursor.execute(create_table_query)
        conn.commit()
        print("coin_overview table created successfully.")
    except Exception as e:
        print(f"Failed to create table: {str(e)}")
        conn.rollback()
    finally:
        cursor.close()
     
# Table insertion
def insert_stock_historical_price_table(conn, file_path):
    cursor = conn.cursor()
    try:
        with open(file_path, 'r') as file:
            data = json.load(file)
            
            symbol = data["Meta Data"]["2. Symbol"]
            time_series = data["Time Series (Daily)"]
            
            records = []
            for date_str, metrics in time_series.items():
                record = (
                    symbol,
                    datetime.strptime(date_str, '%Y-%m-%d'),
                    float(metrics["1. open"]),
                    float(metrics["2. high"]),
                    float(metrics["3. low"]),
                    float(metrics["4. close"]),
                    int(metrics["6. volume"])
                )
                records.append(record)
            
            # Insert data into the database
            insert_query = """
            INSERT INTO stock_historical_price (symbol, date, open, high, low, close, volume)
            VALUES %s
            ON CONFLICT (symbol, date)
            DO UPDATE SET 
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume
            WHERE stock_historical_price.open <> EXCLUDED.open
               OR stock_historical_price.high <> EXCLUDED.high
               OR stock_historical_price.low <> EXCLUDED.low
               OR stock_historical_price.close <> EXCLUDED.close
               OR stock_historical_price.volume <> EXCLUDED.volume;
            """
            execute_values(cursor, insert_query, records)
            conn.commit()
    except Exception as e:
        print(f"Failed to insert data from {file_path}: {e}")
        conn.rollback()
    finally:
        cursor.close()

def insert_coin_historical_price_table(conn, file_path):
    cursor = conn.cursor()
    try: 
        with open(file_path, 'r') as file:
            data = json.load(file)
            symbol = os.path.splitext(os.path.basename(file_path))[0]
            extracted_data = []
            seen_dates = set()
            for entry in data:
                date = pd.to_datetime(entry[0], unit='ms').strftime('%Y-%m-%d')
                open_price  = entry[1]
                high = entry[2]
                low = entry[3]
                close = entry[4]
                               
                if date not in seen_dates:# this is to deal with gc duplicated data that cause errors sometimes
                    extracted_data.append([symbol, date, open_price, high, low, close])
                    seen_dates.add(date)
            
            # Insert data into the database
            insert_query = """
            INSERT INTO coin_historical_price (symbol, date, open, high, low, close)
            VALUES %s
            ON CONFLICT (symbol, date)
            DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close
            WHERE coin_historical_price.open <> EXCLUDED.open
               OR coin_historical_price.high <> EXCLUDED.high
               OR coin_historical_price.low <> EXCLUDED.low
               OR coin_historical_price.close <> EXCLUDED.close;
            """
            execute_values(cursor, insert_query, extracted_data)
            conn.commit()
    except Exception as e:
        print(f"Failed to insert data from {file_path}: {e}")
        conn.rollback()
    finally:
        cursor.close()

def insert_coin_hourly_historical_price_table(conn, file_path):
    cursor = conn.cursor()
    try: 
        with open(file_path, 'r') as file:
            data = json.load(file)
            symbol = os.path.splitext(os.path.basename(file_path))[0]
            extracted_data = []
            seen_dates = set()
            for entry in data:
                date = pd.to_datetime(entry[0], unit='ms').strftime('%Y-%m-%d')
                open_price  = entry[1]
                high = entry[2]
                low = entry[3]
                close = entry[4]
                               
                if date not in seen_dates:# this is to deal with gc duplicated data that cause errors sometimes
                    extracted_data.append([symbol, date, open_price, high, low, close])
                    seen_dates.add(date)
            
            # Insert data into the database
            insert_query = """
            INSERT INTO coin_hourly_historical_price (symbol, date, open, high, low, close)
            VALUES %s
            ON CONFLICT (symbol, date)
            DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close
            WHERE coin_hourly_historical_price.open <> EXCLUDED.open
               OR coin_hourly_historical_price.high <> EXCLUDED.high
               OR coin_hourly_historical_price.low <> EXCLUDED.low
               OR coin_hourly_historical_price.close <> EXCLUDED.close;
            """
            execute_values(cursor, insert_query, extracted_data)
            conn.commit()
    except Exception as e:
        print(f"Failed to insert data from {file_path}: {e}")
        conn.rollback()
    finally:
        cursor.close()
        
def insert_stock_overview_table(conn, file_path):
    cursor = conn.cursor()
    try:
        with open(file_path, 'r') as file:
            data = json.load(file)

        required_keys = ["Symbol", "AssetType", "Name", "Description", "CIK", "Exchange", "Currency", "Country", 
        "Sector", "Industry", "Address", "FiscalYearEnd", "LatestQuarter", "MarketCapitalization", 
        "EBITDA", "PERatio", "PEGRatio", "BookValue", "DividendPerShare", "DividendYield", 
        "EPS", "RevenuePerShareTTM", "ProfitMargin", "OperatingMarginTTM", "ReturnOnAssetsTTM", 
        "ReturnOnEquityTTM", "RevenueTTM", "GrossProfitTTM", "DilutedEPSTTM", 
        "QuarterlyEarningsGrowthYOY", "QuarterlyRevenueGrowthYOY", "AnalystTargetPrice", 
        "AnalystRatingStrongBuy", "AnalystRatingBuy", "AnalystRatingHold", "AnalystRatingSell", 
        "AnalystRatingStrongSell", "TrailingPE", "ForwardPE", "PriceToSalesRatioTTM", 
        "PriceToBookRatio", "EVToRevenue", "EVToEBITDA", "Beta", "52WeekHigh", "52WeekLow", 
        "50DayMovingAverage", "200DayMovingAverage", "SharesOutstanding", "DividendDate", "ExDividendDate"]
        missing_keys = [key for key in required_keys if key not in data]

        if missing_keys:
            raise KeyError(f"Missing keys in the JSON data: {', '.join(missing_keys)}")
     
        record = (
            truncate_string(data["Symbol"], 10),
            truncate_string(data["AssetType"], 50),
            truncate_string(data["Name"], 255),
            data["Description"],  
            truncate_string(data["CIK"], 10),
            truncate_string(data["Exchange"], 10),
            truncate_string(data["Currency"], 10),
            truncate_string(data["Country"], 50),
            truncate_string(data["Sector"], 50),
            truncate_string(data["Industry"], 255),
            truncate_string(data["Address"], 255),
            truncate_string(data["FiscalYearEnd"], 20),
            convert_to_date(data["LatestQuarter"]),
            convert_to_int(data["MarketCapitalization"]),
            convert_to_int(data["EBITDA"]),
            convert_to_float(data["PERatio"]),
            convert_to_float(data["PEGRatio"]),
            convert_to_float(data["BookValue"]),
            convert_to_float(data["DividendPerShare"]),
            convert_to_float(data["DividendYield"]),
            convert_to_float(data["EPS"]),
            convert_to_float(data["RevenuePerShareTTM"]),
            convert_to_float(data["ProfitMargin"]),
            convert_to_float(data["OperatingMarginTTM"]),
            convert_to_float(data["ReturnOnAssetsTTM"]),
            convert_to_float(data["ReturnOnEquityTTM"]),
            convert_to_int(data["RevenueTTM"]),
            convert_to_int(data["GrossProfitTTM"]),
            convert_to_float(data["DilutedEPSTTM"]),
            convert_to_float(data["QuarterlyEarningsGrowthYOY"]),
            convert_to_float(data["QuarterlyRevenueGrowthYOY"]),
            convert_to_float(data["AnalystTargetPrice"]),
            convert_to_int(data["AnalystRatingStrongBuy"]),
            convert_to_int(data["AnalystRatingBuy"]),
            convert_to_int(data["AnalystRatingHold"]),
            convert_to_int(data["AnalystRatingSell"]),
            convert_to_int(data["AnalystRatingStrongSell"]),
            convert_to_float(data["TrailingPE"]),
            convert_to_float(data["ForwardPE"]),
            convert_to_float(data["PriceToSalesRatioTTM"]),
            convert_to_float(data["PriceToBookRatio"]),
            convert_to_float(data["EVToRevenue"]),
            convert_to_float(data["EVToEBITDA"]),
            convert_to_float(data["Beta"]),
            convert_to_float(data["52WeekHigh"]),
            convert_to_float(data["52WeekLow"]),
            convert_to_float(data["50DayMovingAverage"]),
            convert_to_float(data["200DayMovingAverage"]),
            convert_to_int(data["SharesOutstanding"]),
            convert_to_date(data["DividendDate"]),
            convert_to_date(data["ExDividendDate"])
        )
        
        # Insert data into the stock_overview table
        insert_query = """
        INSERT INTO stock_overview (
            Symbol, AssetType, Name, Description, CIK, Exchange, Currency, Country, Sector, Industry, Address, FiscalYearEnd, LatestQuarter, 
            MarketCapitalization, EBITDA, PERatio, PEGRatio, BookValue, DividendPerShare, DividendYield, EPS, RevenuePerShareTTM, ProfitMargin, 
            OperatingMarginTTM, ReturnOnAssetsTTM, ReturnOnEquityTTM, RevenueTTM, GrossProfitTTM, DilutedEPSTTM, QuarterlyEarningsGrowthYOY, 
            QuarterlyRevenueGrowthYOY, AnalystTargetPrice, AnalystRatingStrongBuy, AnalystRatingBuy, AnalystRatingHold, AnalystRatingSell, 
            AnalystRatingStrongSell, TrailingPE, ForwardPE, PriceToSalesRatioTTM, PriceToBookRatio, EVToRevenue, EVToEBITDA, Beta, High52Week, 
            Low52Week, MovingAverage50Day, MovingAverage200Day, SharesOutstanding, DividendDate, ExDividendDate
        ) VALUES %s
        ON CONFLICT (Symbol) DO UPDATE SET
            AssetType = EXCLUDED.AssetType,
            Name = EXCLUDED.Name,
            Description = EXCLUDED.Description,
            CIK = EXCLUDED.CIK,
            Exchange = EXCLUDED.Exchange,
            Currency = EXCLUDED.Currency,
            Country = EXCLUDED.Country,
            Sector = EXCLUDED.Sector,
            Industry = EXCLUDED.Industry,
            Address = EXCLUDED.Address,
            FiscalYearEnd = EXCLUDED.FiscalYearEnd,
            LatestQuarter = EXCLUDED.LatestQuarter,
            MarketCapitalization = EXCLUDED.MarketCapitalization,
            EBITDA = EXCLUDED.EBITDA,
            PERatio = EXCLUDED.PERatio,
            PEGRatio = EXCLUDED.PEGRatio,
            BookValue = EXCLUDED.BookValue,
            DividendPerShare = EXCLUDED.DividendPerShare,
            DividendYield = EXCLUDED.DividendYield,
            EPS = EXCLUDED.EPS,
            RevenuePerShareTTM = EXCLUDED.RevenuePerShareTTM,
            ProfitMargin = EXCLUDED.ProfitMargin,
            OperatingMarginTTM = EXCLUDED.OperatingMarginTTM,
            ReturnOnAssetsTTM = EXCLUDED.ReturnOnAssetsTTM,
            ReturnOnEquityTTM = EXCLUDED.ReturnOnEquityTTM,
            RevenueTTM = EXCLUDED.RevenueTTM,
            GrossProfitTTM = EXCLUDED.GrossProfitTTM,
            DilutedEPSTTM = EXCLUDED.DilutedEPSTTM,
            QuarterlyEarningsGrowthYOY = EXCLUDED.QuarterlyEarningsGrowthYOY,
            QuarterlyRevenueGrowthYOY = EXCLUDED.QuarterlyRevenueGrowthYOY,
            AnalystTargetPrice = EXCLUDED.AnalystTargetPrice,
            AnalystRatingStrongBuy = EXCLUDED.AnalystRatingStrongBuy,
            AnalystRatingBuy = EXCLUDED.AnalystRatingBuy,
            AnalystRatingHold = EXCLUDED.AnalystRatingHold,
            AnalystRatingSell = EXCLUDED.AnalystRatingSell,
            AnalystRatingStrongSell = EXCLUDED.AnalystRatingStrongSell,
            TrailingPE = EXCLUDED.TrailingPE,
            ForwardPE = EXCLUDED.ForwardPE,
            PriceToSalesRatioTTM = EXCLUDED.PriceToSalesRatioTTM,
            PriceToBookRatio = EXCLUDED.PriceToBookRatio,
            EVToRevenue = EXCLUDED.EVToRevenue,
            EVToEBITDA = EXCLUDED.EVToEBITDA,
            Beta = EXCLUDED.Beta,
            High52Week = EXCLUDED.High52Week,
            Low52Week = EXCLUDED.Low52Week,
            MovingAverage50Day = EXCLUDED.MovingAverage50Day,
            MovingAverage200Day = EXCLUDED.MovingAverage200Day,
            SharesOutstanding = EXCLUDED.SharesOutstanding,
            DividendDate = EXCLUDED.DividendDate,
            ExDividendDate = EXCLUDED.ExDividendDate
        """
        
        execute_values(cursor, insert_query, [record])
        conn.commit()
    except Exception as e:
        print(f"Failed to insert data at {data['Symbol']}: {e}")
        conn.rollback()
    finally:
        cursor.close()

def insert_coin_overview_table(conn, file_path):
    cursor = conn.cursor()
    try:
        with open(file_path, 'r') as file:
            data = json.load(file)
        for entry in data:
            try:
                record = (
                    entry.get("symbol").upper(),
                    entry.get("name"),
                    convert_to_float(entry.get("current_price")),
                    convert_to_int(entry.get("market_cap")),
                    convert_to_int(entry.get("market_cap_rank")),
                    convert_to_int(entry.get("fully_diluted_valuation")),
                    convert_to_int(entry.get("total_volume")),
                    convert_to_float(entry.get("high_24h")),
                    convert_to_float(entry.get("low_24h")),
                    convert_to_float(entry.get("price_change_24h")),
                    convert_to_float(entry.get("price_change_percentage_24h")),
                    convert_to_int(entry.get("market_cap_change_24h")),
                    convert_to_float(entry.get("market_cap_change_percentage_24h")),
                    convert_to_float(entry.get("circulating_supply")),
                    convert_to_float(entry.get("total_supply")),
                    convert_to_float(entry.get("max_supply")),
                    convert_to_float(entry.get("ath")),
                    convert_to_datetime(entry.get("ath_date")),
                    convert_to_float(entry.get("atl")),
                    convert_to_datetime(entry.get("atl_date")),
                    convert_to_datetime(entry.get("last_updated"))
                ) 
                insert_query = """
                INSERT INTO coin_overview (
                    symbol, name, current_price, market_cap, market_cap_rank,
                    fully_diluted_valuation, total_volume, high_24h, low_24h, price_change_24h, price_change_percentage_24h, market_cap_change_24h, market_cap_change_percentage_24h,
                    circulating_supply, total_supply, max_supply, ath, ath_date,
                    atl, atl_date, last_updated
                ) VALUES %s
                ON CONFLICT (symbol, name)
                DO UPDATE SET
                    current_price = EXCLUDED.current_price,
                    market_cap = EXCLUDED.market_cap,
                    market_cap_rank = EXCLUDED.market_cap_rank,
                    fully_diluted_valuation = EXCLUDED.fully_diluted_valuation,
                    total_volume = EXCLUDED.total_volume,
                    high_24h = EXCLUDED.high_24h,
                    low_24h = EXCLUDED.low_24h,
                    price_change_24h = EXCLUDED.price_change_24h,
                    price_change_percentage_24h = EXCLUDED.price_change_percentage_24h,
                    market_cap_change_24h = EXCLUDED.market_cap_change_24h,
                    market_cap_change_percentage_24h = EXCLUDED.market_cap_change_percentage_24h,
                    circulating_supply = EXCLUDED.circulating_supply,
                    total_supply = EXCLUDED.total_supply,
                    max_supply = EXCLUDED.max_supply,
                    ath = EXCLUDED.ath,
                    ath_date = EXCLUDED.ath_date,
                    atl = EXCLUDED.atl,
                    atl_date = EXCLUDED.atl_date,
                    last_updated = EXCLUDED.last_updated
                """
                cursor.execute(insert_query, (record,))
                conn.commit()
            except Exception as record_error:
                print(f"Failed to insert record for symbol: {entry.get('symbol')} - Error: {record_error}")
                conn.rollback()
    except Exception as e:
        print(f"Failed to insert data: {str(e)}")
        conn.rollback()
    finally:
        cursor.close()

