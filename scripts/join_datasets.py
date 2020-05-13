import pandas as pd

stock_deviations_df = pd.read_csv('stock_deviations_by_day.csv')
noise_complaints_df = pd.read_csv('noise_complaints_by_day.csv')
weather_df = pd.read_csv('weather_by_day.csv')

merged_df = stock_deviations_df.merge(noise_complaints_df, on='date')
merged_df = merged_df.merge(weather_df, on='date')

merged_df.to_csv('stock_weather_311.csv', index=False)
