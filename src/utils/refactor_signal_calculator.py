from abc import ABC, abstractmethod
import os
import json
import warnings
import pandas as pd
from statsmodels.tsa.stattools import coint
import statsmodels.api as sm
from tqdm import tqdm
from config import *
import logging
import numpy as np

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)8s | %(message)s',
    datefmt='%Y-%m-%d %H:%M' #datefmt='%Y-%m-%d %H:%M:%S'
)

'''CALCULATOR'''
class signal_calculator(ABC):
    
    def __init__(self, price_df, output_signal_path):
        self.price_df = price_df
        self.output_signal_path = output_signal_path
        
    @abstractmethod
    def calculate_data(self):
        pass
    
    @abstractmethod
    def calculate_signal(self, output_df):
        pass

class stonewell_signal_calculator(signal_calculator):
    
    CLOSE_SMA_PERIODS = [20, 50, 100, 200]
    VOLUME_SMA_PERIODS = [7, 14, 30]
    RSI_PERIOD = 14
    RSI_SMA_PERIODS = [20, 50]

    def _calculate_rsi(self, data, period):
        delta = data.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        return 100 - (100 / (1 + rs))
        
    def calculate_data(self):
        df = self.price_df.copy()
        
        # Calculate Close SMAs
        for period in self.CLOSE_SMA_PERIODS:
            df[f'close_sma_{period}'] = df.groupby('symbol')['close'].transform(lambda x: x.rolling(window=period).mean())
        
        # Calculate Volume SMAs
        for period in self.VOLUME_SMA_PERIODS:
            df[f'volume_sma_{period}'] = df.groupby('symbol')['volume'].transform(lambda x: x.rolling(window=period).mean())
        
        df['rsi_14'] = df.groupby('symbol')['close'].transform(lambda x: self._calculate_rsi(x, self.RSI_PERIOD))
        
        # Calculate RSI SMAs
        for period in self.RSI_SMA_PERIODS:
            df[f'rsi_14_sma_{period}'] = df.groupby('symbol')['rsi_14'].transform(lambda x: x.rolling(window=period).mean())
        
        return df

    def calculate_signal(self, output_df):
        latest_data = output_df.groupby('symbol').last().reset_index()
        signals = []
        last_updated = latest_data['date'].max()
        
        for _, row in latest_data.iterrows():
            signal = {
                'symbol': row['symbol'],
                'close_above_sma': row['close'] > row['close_sma_20'],
                'close_above_sma_pct': (row['close'] - row['close_sma_20']) / row['close_sma_20'] * 100,  # Percentage above SMA
                'rsi_above_sma': row['rsi_14'] > row['rsi_14_sma_20'],
                'short_vol_above_long': row['volume_sma_7'] > row['volume_sma_30'],
                'death_cross': row['close_sma_200'] > row['close_sma_50'],
                'last_updated': last_updated
            }
            signals.append(signal)
        
        return pd.DataFrame(signals)


class coint_signal_calculator(signal_calculator):
    def __init__(self, price_df, checkpoint_file_path, output_data_path, output_signal_path):
        super().__init__(price_df, output_signal_path)
        self.checkpoint_file_path = checkpoint_file_path
        self.output_data_path = output_data_path 
        
    def _rolling_cointegration(self, name1, data1, name2, data2, window_length):
        warnings.filterwarnings("ignore", category=sm.tools.sm_exceptions.CollinearityWarning)
        if len(data1) < window_length or len(data1) != len(data2):
            raise ValueError("The length of the time window is greater than the available df, OR data lengths differ.")
        
        rolling_p_values = []
        for end in range(window_length, len(data1)):
            start = end - window_length
            series1 = data1[start:end]
            series2 = data2[start:end]
            
            _, p_value, _ = coint(series1, series2, trend='ct')
            rolling_p_values.append(p_value) 

        results = pd.DataFrame({
            'p_value': rolling_p_values
        })
        results.columns = [f'{name1}_{name2}_p_val']

        return results
      
    def calculate_data(self):
        price_df = self.price_df.copy()
        price_df['date'] = pd.to_datetime(price_df['date'])
      
        start_date_index = price_df.index[price_df['date'] >= ROLLING_COINT_START_DATE][0]
        adjusted_start_date_index = max(0, start_date_index - ROLLING_COINT_WINDOW)

        price_df = price_df.iloc[adjusted_start_date_index:]
        date = price_df['date'][ROLLING_COINT_WINDOW:].reset_index(drop=True)
        price_df = price_df.drop('date', axis=1)

        # save progress of analyzed coin pairs
        if os.path.exists(self.checkpoint_file_path):
            with open(self.checkpoint_file_path, 'r') as file:
                checkpoint_data = json.load(file)
        else:
            checkpoint_data = []
        results = pd.DataFrame(date)
        # get rolling coint for all possible pairs
        for i in range(price_df.shape[1]):
            name1 = price_df.columns[i]
            data1 = price_df.iloc[:, i]
            logging.info(f'--- {name1} - getting all possible pairs')
            for j in range(i+1, price_df.shape[1]):
                # save progress
                name2 = price_df.columns[j]      
                if [name1, name2] in checkpoint_data:
                    logging.info(f"Skip pair {name1} X {name2} since already ran")
                    continue
                # try rolling cointegration
                try:
                    data2 = price_df.iloc[:, j]
                    res = self._rolling_cointegration(name1, data1, name2, data2, ROLLING_COINT_WINDOW)
                    results = pd.concat([results, res], axis=1)
                    checkpoint_data.append([name1, name2])
                except Exception as e:
                    logging.error(f'Error processing {name1} X {name2}: {e}')
                    continue
            try:
                # periodically save all results to csv and checkpoint file
                with open(self.checkpoint_file_path, 'w') as file:
                    json.dump(checkpoint_data, file, indent=4)
                try:
                    results.to_csv(self.output_data_path, index=False) 
                except Exception as e:  
                    logging.error(f"Error saving to CSV: {e} but continue")    
            except Exception as e:
                logging.error(f"Error saving checkpoint: {e} but continue")
                continue

        # save final results to csv
        with open(self.checkpoint_file_path, 'w') as file:
            json.dump(checkpoint_data, file, indent=4)
        results.to_csv(self.output_data_path, index=False)
        logging.info("Results saved to CSV")
        return results  
       
    def transform_data(self, df):
        try:
            df.columns = df.columns.str.replace('_p_val$', '', regex=True)
            df_melted = pd.melt(df, id_vars=['date'], var_name='pair_name', value_name='value')
            df_melted[['symbol1', 'symbol2']] = df_melted['pair_name'].str.split('_', expand=True)
            df_melted = df_melted.drop(columns=['pair_name'])
            df_melted['window_length'] = ROLLING_COINT_WINDOW
            df_melted = df_melted[['date', 'window_length', 'symbol1', 'symbol2', 'value']]
            return df_melted
        except Exception as e:
            logging.error(f"Error in transform_data: {str(e)}")
            import sys
            sys.exit("Stopping script due to error in transform_data")
        
    def _coint_pct_eval(self, df, hist_len, recent_len, pval=0.05):
        hist_coint_cnt = []
        hist_tlt_cnt = []
        most_recent_coint_cnt = []
        most_recent_tlt_cnt = []
        recent_coint_cnt = []
        recent_tlt_cnt = []
        cols = []

        for col in df.columns[1:]:
            hist_len = min(hist_len, len(df[col]))
            hist_coint_cnt.append(int((df[col][-hist_len:] <= pval).sum()))
            hist_tlt_cnt.append(len(df[col][-hist_len:]))
            
            recent_len = min(recent_len, len(df[col]))
            recent_coint_cnt.append(int((df[col][-recent_len:] <= pval).sum()))
            recent_tlt_cnt.append(len(df[col][-recent_len:]))

            most_recent_len = 14
            most_recent_len = min(most_recent_len, len(df[col]))
            most_recent_coint_cnt.append(int((df[col][-most_recent_len:] <= pval).sum()))
            most_recent_tlt_cnt.append(len(df[col][-most_recent_len:]))
            cols.append(col)

        result_df = pd.DataFrame({
            'name': cols,
            'hist_coint_cnt': hist_coint_cnt,
            'hist_tlt_cnt': hist_tlt_cnt,
            'recent_coint_cnt': recent_coint_cnt,
            'recent_tlt_cnt': recent_tlt_cnt,
            'most_recent_coint_cnt': most_recent_coint_cnt,
            'most_recent_tlt_cnt': most_recent_tlt_cnt,
        })

        result_df['hist_coint_pct'] = result_df['hist_coint_cnt'] / result_df['hist_tlt_cnt']
        result_df['recent_coint_pct'] = result_df['recent_coint_cnt'] / result_df['recent_tlt_cnt']
        result_df['most_recent_coint_pct'] = (result_df['most_recent_coint_cnt'] / result_df['most_recent_tlt_cnt'])  
        result_df = result_df[['name', 'most_recent_coint_pct', 'recent_coint_pct', 'hist_coint_pct']]

        return result_df
      
    def _get_ols_coeff(self, name1, name2, series1, series2):
        if series1.std() == 0 or series2.std() == 0:
            logging.warning(f"Warning: Constant series detected for {name1} or {name2}")
            return None

        # Check for perfect correlation
        if abs(series1.corr(series2)) > 0.9999:
            logging.warning(f"Warning: Near-perfect correlation detected between {name1} and {name2}")
            return None

        try:
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", message="divide by zero encountered in scalar divide")
                ols_result = sm.OLS(series1, sm.add_constant(series2)).fit()
            
            # Check if R-squared is valid
            if np.isnan(ols_result.rsquared_adj) or np.isinf(ols_result.rsquared_adj):
                logging.warning(f"Warning: Invalid R-squared for {name1} and {name2}")
                return None
            
            return {
                'name1': name1, 
                'name2': name2,
                'ols_constant': ols_result.params.iloc[0],  # Intercept
                'ols_coeff': ols_result.params.iloc[1],  # coeff
                'r_squared': ols_result.rsquared_adj  
            }
        except Exception as e:
            logging.error(f"Error in OLS calculation for {name1} and {name2}: {str(e)}")
            return None
        
    def _get_multi_pairs_ols_coeff(self, hist_price_df, col_name):
        hist_price_df = hist_price_df.iloc[-OLS_WINDOW:] # use last 120 days to get coeff
        last_updated = hist_price_df['date'].iloc[-1]
        hist_price_df = hist_price_df.drop('date', axis=1)
        result = []
        logging.info('---Begin getting ols for pairs')
        for pair in tqdm(col_name):
            split_string = pair.split('_')
            symbol1 = split_string[0]
            symbol2 = split_string[1]
            
            if symbol1 not in hist_price_df.columns or symbol2 not in hist_price_df.columns:
                logging.warning(f"Skipping pair {pair}: Columns {symbol1} or {symbol2} are missing in hist_price_df")
                continue
            
            ols = self._get_ols_coeff(symbol1, symbol2, hist_price_df.loc[:, symbol1], hist_price_df.loc[:, symbol2])
            
            if ols is not None: 
                ols['last_updated'] = last_updated
                result.append(ols)
            
        return pd.DataFrame(result)
    
    def calculate_signal(self, output_df):
        # rolling coint scores
        signal_df = self._coint_pct_eval(output_df, HIST_WINDOW_SIG_EVAL, RECENT_WINDOW_SIG_EVAL)

        # ols params for trading spread
        ols_df = self._get_multi_pairs_ols_coeff(self.price_df, signal_df['name'])

        results = pd.concat([signal_df.reset_index(drop=True), ols_df.reset_index(drop=True)], axis=1)
        results['window_length'] = ROLLING_COINT_WINDOW
        results = results[['name1', 'name2', 'window_length', 'most_recent_coint_pct', 'recent_coint_pct', 'hist_coint_pct', 'r_squared', 'ols_constant', 'ols_coeff', 'last_updated']]
        results.to_csv(self.output_signal_path, index=False)
        # reorder the data
        return results