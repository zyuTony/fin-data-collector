import pandas as pd
import os
import json
from config import *
from statsmodels.tsa.stattools import coint
import statsmodels.api as sm
from config import *
from tqdm import tqdm 
from multiprocessing import Pool
import time 

import warnings
from statsmodels.tools.sm_exceptions import CollinearityWarning

# Get rolling coint
def rolling_cointegration(name1, data1, name2, data2, window_length):
    warnings.filterwarnings("ignore", category=CollinearityWarning)
    if len(data1) < window_length or len(data1) != len(data2) :
        raise ValueError("The length of the time window is greater than the available df, OR data lengths differ.")
    
    rolling_coint_scores = []
    rolling_p_values = []

    for end in range(window_length, len(data1)):
        start = end - window_length
        series1 = data1[start:end]
        series2 = data2[start:end]
        
        score, p_value, _ = coint(series1, series2, trend='ct')
        rolling_coint_scores.append(score)
        rolling_p_values.append(p_value) 

    # results = pd.DataFrame({
    #     'coint_score': rolling_coint_scores,
    #     'p_value': rolling_p_values
    # })
    # results.columns = [f'{name1}_{name2}_coint_score', f'{name1}_{name2}_p_val']
    
    results = pd.DataFrame({
        'p_value': rolling_p_values
    })
    results.columns = [f'{name1}_{name2}_p_val']

    return results

def save_multi_pairs_rolling_coint(price_df, top_n_data, checkpoint_file, result_file):
    price_df['date'] = pd.to_datetime(price_df['date'])

    start_date_index = price_df.index[price_df['date'] >= ROLLING_COINT_START_DATE][0]
    adjusted_start_date_index = max(0, start_date_index - ROLLING_COINT_WINDOW)

    price_df = price_df.iloc[adjusted_start_date_index:]
    date = price_df['date'][ROLLING_COINT_WINDOW:].reset_index(drop=True)
    price_df = price_df.drop('date', axis=1)
    if top_n_data: # if not none, make sure to not go overindex
        top_n_data = min(top_n_data, len(price_df.columns))
    price_df = price_df.iloc[:,:top_n_data] # when none, do all pairs

    # save progress of analyzed coin pairs
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, 'r') as file:
            checkpoint_data = json.load(file)
    else:
        checkpoint_data = []
    results = pd.DataFrame(date)
    # get rolling coint for all possible pairs
    for i in range(price_df.shape[1]):
        name1 = price_df.columns[i]
        data1 = price_df.iloc[:, i]
        print(f'--- {name1} - getting all possible pairs')
        for j in range(i+1, price_df.shape[1]):
            # save progress
            name2 = price_df.columns[j]      
            if [name1, name2] in checkpoint_data:
                print(f"Skip pair {name1} X {name2} since already ran")
                continue
            # try rolling cointegration
            try:
                data2 = price_df.iloc[:, j]
                # print(f'Working on {name1} X {name2}')
                res = rolling_cointegration(name1, data1, name2, data2, ROLLING_COINT_WINDOW)
                results = pd.concat([results, res], axis=1)
                checkpoint_data.append([name1, name2])
            except Exception as e:
                print(f'Error processing {name1} X {name2}: {e}')
                continue
        try:
            # periodically save all results to csv and checkpoint file
            with open(checkpoint_file, 'w') as file:
                json.dump(checkpoint_data, file, indent=4)
            try:
                results.to_csv(result_file, index=False)
            except Exception as e:  
                print(f"Error: {e} but continue")    
        except Exception as e:
            print(f"Error: {e} but continue")
            continue

    # save final results to csv
    with open(checkpoint_file, 'w') as file:
                json.dump(checkpoint_data, file, indent=4)
    results.to_csv(result_file, index=False)
    return results

def save_target_symbol_rolling_coint(target_symbol, price_df, top_n_data, checkpoint_file, result_file):
    price_df['date'] = pd.to_datetime(price_df['date'])

    start_date_index = price_df.index[price_df['date'] >= ROLLING_COINT_START_DATE][0]
    adjusted_start_date_index = max(0, start_date_index - ROLLING_COINT_WINDOW)

    price_df = price_df.iloc[adjusted_start_date_index:]
    date = price_df['date'][ROLLING_COINT_WINDOW:].reset_index(drop=True)
    price_df = price_df.drop('date', axis=1)
    if top_n_data: # if not none, make sure to not go overindex
        top_n_data = min(top_n_data, len(price_df.columns))
    price_df = price_df.iloc[:,:top_n_data] # when none, do all pairs

    # save progress of analyzed coin pairs
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, 'r') as file:
            checkpoint_data = json.load(file)
    else:
        checkpoint_data = []
    results = pd.DataFrame(date)
    # get rolling coint for all possible pairs
    target_data = price_df[target_symbol]
    for i in tqdm(range(price_df.shape[1])):
        print(f'--- {target_symbol}')
        # save progress
        name2 = price_df.columns[i]      
        if [target_symbol, name2] in checkpoint_data:
            print(f"Skip pair {target_symbol} X {name2} since already ran")
            continue
        # try rolling cointegration
        try:
            data2 = price_df.iloc[:, i]
            # print(f'Working on {target_symbol} X {name2}')
            res = rolling_cointegration(target_symbol, target_data, name2, data2, ROLLING_COINT_WINDOW)
            results = pd.concat([results, res], axis=1)
            checkpoint_data.append([target_symbol, name2])
            if i%10 == 0:
                try:# periodically save all results to csv and checkpoint file
                    with open(checkpoint_file, 'w') as file:
                        json.dump(checkpoint_data, file, indent=4)
                    try:
                        results.to_csv(result_file, index=False)
                        print('save to csv')
                    except Exception as e:  
                        print(f"Error: {e} but continue")    
                except Exception as e:
                    print(f"Error: {e} but continue")
                    continue

        except Exception as e:
            print(f'Error processing {target_symbol} X {name2}: {e}')
            continue        

    # save final results to csv
    with open(checkpoint_file, 'w') as file:
                json.dump(checkpoint_data, file, indent=4)
    results.to_csv(result_file, index=False)
    return results

def coint_pct_eval(df, hist_len, recent_len, pval=0.05):
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
 
def get_ols_coeff(name1, name2, series1, series2):
    ols_result = sm.OLS(series1, sm.add_constant(series2)).fit() 
    return {
        'name1': name1, 
        'name2': name2,
        'ols_constant': ols_result.params.iloc[0],  # Intercept
        'ols_coeff': ols_result.params.iloc[1],  # coeff
        'r_squared': ols_result.rsquared_adj  
    }

def get_multi_pairs_ols_coeff(hist_price_df, col_name):
    hist_price_df = hist_price_df.iloc[-OLS_WINDOW:] # use last 120 days to get coeff
    last_updated = hist_price_df['date'].iloc[-1]
    hist_price_df = hist_price_df.drop('date', axis=1)
    result = []
    print('---Begin getting ols for pairs')
    for pair in tqdm(col_name):
        split_string = pair.split('_')
        symbol1 = split_string[0]
        symbol2 = split_string[1]
        
        if symbol1 not in hist_price_df.columns or symbol2 not in hist_price_df.columns:
            print(f"Skipping pair {pair}: Columns {symbol1} or {symbol2} are missing in hist_price_df")
            continue
        
        ols = get_ols_coeff(symbol1, symbol2, hist_price_df.loc[:, symbol1], hist_price_df.loc[:, symbol2])
        ols['last_updated'] = last_updated
        result.append(ols)
        
    return pd.DataFrame(result)

def get_signal(rolling_coint_df, hist_price_df):
    # rolling coint scores
    signal_df = coint_pct_eval(rolling_coint_df, HIST_WINDOW_SIG_EVAL, RECENT_WINDOW_SIG_EVAL)

    # ols params for trading spread
    ols_df = get_multi_pairs_ols_coeff(hist_price_df, signal_df['name'])

    result = pd.concat([signal_df.reset_index(drop=True), ols_df.reset_index(drop=True)], axis=1)
    result['window_length'] = ROLLING_COINT_WINDOW
    # reorder the data
    return result[['name1', 'name2', 'window_length', 'most_recent_coint_pct', 'recent_coint_pct', 'hist_coint_pct', 'r_squared', 'ols_constant', 'ols_coeff', 'last_updated']]
 

#### NOT IN USE #####
# Get signal/ols from coint
def min_cont_coint_check(df, x, threshold=0.05): 
    result = []
    for col in df.columns[1:]:
        series = df[col] <= threshold
        if any(series.rolling(window=x).sum() == x):
            result.append(col)
    return result
