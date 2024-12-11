import numpy as np

def add_signals_simple_strat(df):
    short_window = 40
    long_window = 100

    df['short_sma'] = df['price'].rolling(window=short_window, min_periods=1).mean()
    df['long_sma'] = df['price'].rolling(window=long_window, min_periods=1).mean()
    
    df['opening_signal'] = 0
    df['closing_signal'] = 0
    # np.select -> [conditions], [outcomes], [else-ers]
    df.loc[df.index[short_window:], 'opening_signal'] = np.where(
        (df['short_sma'][short_window:] > df['long_sma'][short_window:]) &
        (df['short_sma'][short_window:].shift() <= df['long_sma'][short_window:].shift()), 
        1, 
        0
    )

    df.loc[df.index[short_window:], 'closing_signal'] = np.where(
        (df['short_sma'][short_window:] < df['long_sma'][short_window:]) &
        (df['short_sma'][short_window:].shift() >= df['long_sma'][short_window:].shift()), 
        1, 
        0
    )
    
    return df

def add_signals_bb_band(df):
    short_window = 40
    long_window = 100
    
    df['opening_signal'] = 0
    df['closing_signal'] = 0
    
    df.loc[df.index[short_window:], 'opening_signal'] = np.where(
        (df['short_sma'][short_window:] > df['long_sma'][short_window:]) &
        (df['short_sma'][short_window:].shift() <= df['long_sma'][short_window:].shift()), 
        1, 
        0
    )

    df.loc[df.index[short_window:], 'closing_signal'] = np.where(
        (df['short_sma'][short_window:] < df['long_sma'][short_window:]) &
        (df['short_sma'][short_window:].shift() >= df['long_sma'][short_window:].shift()), 
        1, 
        0
    )
    
    return df


def add_signals_spread_zero(df):
    short_window = 40
    long_window = 100

    df['short_sma'] = df['price'].rolling(window=short_window, min_periods=1).mean()
    df['long_sma'] = df['price'].rolling(window=long_window, min_periods=1).mean()
    
    df['opening_signal'] = 0
    df['closing_signal'] = 0
    # np.select -> [conditions], [outcomes], [else-ers]
    df.loc[df.index[short_window:], 'opening_signal'] = np.where(
        (df['short_sma'][short_window:] > df['long_sma'][short_window:]) &
        (df['short_sma'][short_window:].shift() <= df['long_sma'][short_window:].shift()), 
        1, 
        0
    )

    df.loc[df.index[short_window:], 'closing_signal'] = np.where(
        (df['short_sma'][short_window:] < df['long_sma'][short_window:]) &
        (df['short_sma'][short_window:].shift() >= df['long_sma'][short_window:].shift()), 
        1, 
        0
    )
    
    return df