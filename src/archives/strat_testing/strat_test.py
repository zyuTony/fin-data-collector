import pandas as pd
import numpy as np
from financial_database.backend.archives.strat_utils import *
import json 

file = '.home/ec2-user/financial_database/backend/SPY_DAILY_data.csv'

# Load the historical coin data
df = pd.read_csv(file, parse_dates=['date'])
df.set_index('date', inplace=True)

"""
MAIN SIGNAL
"""
df = add_signals_simple_strat(df)

'''
POSTIONING AND CLOSING
'''
sizing = 0.1   
init_cap = 100000.0
commission_perc = 0 

max_open_position = 0

total_trades = 0
profit_trades = 0
loss_trades = 0
total_profit_amount = 0
total_loss_amount = 0
open_trades = []
completed_trades = []

df['invested_dollars'] = 0.0
df['open_positions'] = 0
df['shares'] = 0.0
df['capital'] = 0.0

df['current_value'] = 0.0
df['account_value'] = 0.0
df['unrealized_profit'] = 0.0
df.loc[df.index[0], 'capital'] = init_cap

#TODO: add trading cost
#TODO: add stop loss
#TODO: add multiple sample period testing

for i in range(1, len(df)):
    # default update
    df.loc[df.index[i], 'invested_dollars'] = df.loc[df.index[i-1], 'invested_dollars'] 
    df.loc[df.index[i], 'open_positions'] = df.loc[df.index[i-1], 'open_positions'] 
    df.loc[df.index[i], 'capital'] = df.loc[df.index[i-1], 'capital'] 
    df.loc[df.index[i], 'shares'] = df.loc[df.index[i-1], 'shares']  
    # calc current value
    df.loc[df.index[i], 'current_value'] = df.loc[df.index[i],'price'] * df.loc[df.index[i], 'shares']
    df.loc[df.index[i], 'unrealized_profit'] = df.loc[df.index[i],'price'] * df.loc[df.index[i], 'shares'] - df.loc[df.index[i], 'invested_dollars']
    df.loc[df.index[i], 'account_value'] = df.loc[df.index[i],'price'] * df.loc[df.index[i], 'shares'] + df.loc[df.index[i], 'capital']

    # if buy signal and have cash
    if df.loc[df.index[i],'opening_signal'] == 1 and df.loc[df.index[i], 'capital'] >= sizing*init_cap:
        print(f'opening trigger at {i}')
        # calc from previous
        df.loc[df.index[i], 'invested_dollars'] += sizing * init_cap
        df.loc[df.index[i], 'open_positions'] += 1
        df.loc[df.index[i], 'capital'] -= (sizing + commission_perc/100) * init_cap
        df.loc[df.index[i], 'shares'] += df.loc[df.index[i], 'invested_dollars']/df.loc[df.index[i],'price']
        # calc current value
        df.loc[df.index[i], 'current_value'] = df.loc[df.index[i],'price'] * df.loc[df.index[i], 'shares']
        df.loc[df.index[i], 'unrealized_profit'] = df.loc[df.index[i],'price'] * df.loc[df.index[i], 'shares'] - df.loc[df.index[i], 'invested_dollars']
        df.loc[df.index[i], 'account_value'] = df.loc[df.index[i],'price'] * df.loc[df.index[i], 'shares'] + df.loc[df.index[i], 'capital']
        
        # track the num of trade
        open_trades.append({
            'entry_index': i,
            'entry_price': df.loc[df.index[i],'price'],
            'invested_dollars': sizing * init_cap,
            'shares': sizing * init_cap / df.loc[df.index[i],'price']
        })

    # if sell signal and have shares
    elif (df.loc[df.index[i],'shares'] > 0) and (df.loc[df.index[i],'closing_signal'] == 1):
        print(f'closing trigger at {i}')
        # calc from previous
        df.loc[df.index[i], 'capital'] += df.loc[df.index[i], 'shares'] * df.loc[df.index[i],'price'] * (1 - commission_perc/100)
        df.loc[df.index[i], 'invested_dollars'] = 0
        df.loc[df.index[i], 'open_positions'] = 0
        df.loc[df.index[i], 'shares'] = 0
        # calc current value
        df.loc[df.index[i], 'current_value'] = df.loc[df.index[i],'price'] * df.loc[df.index[i], 'shares']
        df.loc[df.index[i], 'unrealized_profit'] = df.loc[df.index[i],'price'] * df.loc[df.index[i], 'shares'] - df.loc[df.index[i], 'invested_dollars']
        df.loc[df.index[i], 'account_value'] = df.loc[df.index[i],'price'] * df.loc[df.index[i], 'shares'] + df.loc[df.index[i], 'capital']

        # track the num of trade
        while open_trades:
            trade = open_trades.pop(0)
            total_trades += 1
            profit_loss = trade['shares'] * df.loc[df.index[i],'price'] * (1 - commission_perc/100) - trade['invested_dollars']
            if profit_loss > 0:
                profit_trades += 1
                total_profit_amount += profit_loss
            else:
                loss_trades += 1
                total_loss_amount += abs(profit_loss)
            completed_trades.append({
            'entry_index': trade['entry_index'],
            'entry_price': trade['entry_price'],
            'sold_price': df.loc[df.index[i],'price'],
            'profit_loss': profit_loss,
            'invested_dollars': trade['invested_dollars'],
            'shares': trade['shares']
        })

with open('completed_trades.json', 'w') as file:
    json.dump(completed_trades, file, indent=4)

df_completed_trades = pd.DataFrame(completed_trades)
df_completed_trades.to_csv('completed_trades.csv', index=False)

print(f'Total Trades: {total_trades}')
print(f'  Profit Trades: {profit_trades}')
print(f'    Avg Profit/Trade: ${total_profit_amount/profit_trades:.7f}')
print(f'  Loss Trades  : {loss_trades}')
print(f'    Avg Loss/Trade  : ${total_loss_amount/loss_trades:.7f}')
print(f'Total Profit Amount: ${total_profit_amount:.1f}')
print(f'Total Loss Amount  : ${total_loss_amount:.1f}')
print(f'Profit Factor      : {total_profit_amount / total_loss_amount:.1f}')


# show strat returns
strat_returns = (df.loc[df.index[-1], 'account_value'] - init_cap) 
strat_returns_perc = (df.loc[df.index[-1], 'account_value'] - init_cap)/init_cap

# calculate HODL returns - buy all at start
HODL_returns = (init_cap/df.loc[df.index[0],'price'] * df.loc[df.index[-1],'price'] - init_cap) 
HODL_returns_perc = (init_cap/df.loc[df.index[0],'price'] * df.loc[df.index[-1],'price'] - init_cap)/init_cap
HODL_returns_avg_price = df.loc[df.index[0],'price']

# calculate DCA returns - buy 10 times
dca_times = 10 
period_length = len(df) // dca_times
dca_index = [i*period_length for i in range(dca_times)]
total_shares = 0
for i in dca_index:
    total_shares += (init_cap/dca_times) / df.loc[df.index[i], 'price']

DCA_returns = (total_shares * df.loc[df.index[-1],'price'] - init_cap)
DCA_returns_perc = (total_shares * df.loc[df.index[-1],'price'] - init_cap)/init_cap
DCA_returns_avg_price = init_cap/total_shares

print(f'stragegy return: ${strat_returns:.0f} ({strat_returns_perc * 100:.1f}%)')
print(f'HODL return: ${HODL_returns:.0f} ({HODL_returns_perc * 100:.1f}%) with avg price {HODL_returns_avg_price:.3f}')
print(f'DCA return: ${DCA_returns:.0f} ({DCA_returns_perc * 100:.1f}%) with avg price {DCA_returns_avg_price:.3f}')
 
# get trading performance - total number of trades, number of trades profit/lost, profit factor - total dollar amount profit/lost  

df.to_excel('./btc_strat_test.xlsx')



