from requests import Request, Session
import json
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import datetime
from statsmodels.tsa.stattools import coint
import statsmodels.api as sm

def pull_coin_list(num_of_coins, api_key):
    url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'
    parameters = {
      'limit':num_of_coins,
      'sort':'market_cap',
      'sort_dir':'desc',
    }

    headers = {
      'Accepts': 'application/json',
      'X-CMC_PRO_API_KEY': api_key,
    }

    session = Session()
    session.headers.update(headers)

    try:
      response = session.get(url, params=parameters)
      data = response.json()
      return data
    except (ConnectionError, Timeout, TooManyRedirects) as e:
      print(e)
      return -1
    
def unix_to_human(unix_timestamp):
    return datetime.datetime.fromtimestamp(unix_timestamp).strftime('%Y-%m-%d')

def human_to_unix(human_date, date_format='%Y-%m-%d'):
    dt = datetime.datetime.strptime(human_date, date_format)
    return int(dt.timestamp())

def coin_list_json_to_str(data):
    ids = ','.join(str(item['id']) for item in data['data'])
    symbols = ','.join(item['symbol'] for item in data['data'])
    return ids, symbols

def coin_list_json_to_array(data):
    ids = [str(item['id']) for item in data['data']]
    symbols = [item['symbol'] for item in data['data']]
    return ids, symbols

def pull_historical_price(coin_ids, api_key, file_location, interval, start_date, end_date):
    url = 'https://pro-api.coinmarketcap.com/v3/cryptocurrency/quotes/historical'
    parameters = {
      'id':coin_ids,
      'interval':interval,
      'time_start':human_to_unix(start_date),
      'time_end':human_to_unix(end_date)
    }

    headers = {
      'Accepts': 'application/json',
      'X-CMC_PRO_API_KEY': api_key,
    }

    session = Session()
    session.headers.update(headers)

    try:
      response = session.get(url, params=parameters)
      data = response.json()
      with open(file_location, 'w') as file:
          json.dump(data, file, indent=4)
      print(f"Data saved to {file_location}")
      return data
    except (ConnectionError, Timeout, TooManyRedirects) as e:
      print(e)
      return -1
  
def run_cointegration_tests(df):
    results = []
    n = df.shape[1]
    for i in range(n):
        for j in range(i+1, n):
            coin1 = df.columns[i]
            coin2 = df.columns[j]
            series1 = df.iloc[:, i]
            series2 = df.iloc[:, j]
            
            # Perform OLS regression
            ols_result = sm.OLS(series1, sm.add_constant(series2)).fit()
            
            # Perform cointegration test
            score, p_value, _ = coint(series1, series2)
            
            # Append the results with regression parameters
            results.append({
                'Coin1': coin1, 
                'Coin2': coin2,
                'Score': score, # Engle test score. Smaller means more cointegrated.
                'P-Value': p_value, # p value of regression residual. <=0.05 for significant
                'OLS-Constant': ols_result.params.iloc[0],  # Intercept
                'OLS-Coefficient': ols_result.params.iloc[1],  # Slope
                'R-squared': ols_result.rsquared,  # R-squared
                'Adjusted-R-squared': ols_result.rsquared_adj  # Adjusted R-squared
            })
        print(f'Finished cointegration test for {coin1} and {coin2}')
    return results