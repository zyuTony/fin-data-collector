import pandas as pd
import numpy as np
import os

def read_time_series(file_path):
    # Read the CSV file
    df = pd.read_csv(file_path)
    return df

def generate_samples(df, length, num_samples):
    # Automatically detect the start and end dates
    start_date = df['date'].min()
    end_date = df['date'].max()
    
    # Ensure the dataframe is sorted by datetime
    df = df.sort_values(by='date').reset_index(drop=True)
    
    total_length = len(df)
    samples = []
    
    if length > total_length:
        raise ValueError("Length of each sample is greater than the total length of the time series data.")
    
    for _ in range(num_samples):
        start_index = np.random.randint(0, total_length - length)
        sample = df.iloc[start_index:start_index + length]
        samples.append(sample)
    
    return samples

def save_samples_to_csv(samples):
    if not os.path.exists('samples'):
        os.makedirs('samples')
        
    for i, sample in enumerate(samples):
        sample.to_csv(f'.home/ec2-user/financial_database/backend/sample_{i+1}.csv', index=False)

# Example usage
file_path = './btc.csv'
length = 1440  # 30 days for 30mins candle
num_samples = 5

# Read the time series data
df = read_time_series(file_path)

# Generate samples
samples = generate_samples(df, length, num_samples)

# Save samples to CSV
save_samples_to_csv(samples)
