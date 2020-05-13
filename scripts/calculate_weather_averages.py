import pandas as pd

# Read data, split into temperature and precipitation DataFrames
df = pd.read_csv('combined_weather_csv_2006-2020.csv')
temps_df = df.drop(['station_location', 'prcp'], axis=1)
prcp_df = df.drop(['station_location', 'tmax', 'tmin', 'tavg'], axis=1)

# Group by date, determine averages and whether there was precipitation        
averages = temps_df.groupby('date').mean()
prcp_flags = prcp_df.groupby('date').any()

# Write to output file
with open('weather_by_day.csv', 'w') as outfile:
    outfile.write('date,tmin,tmax,tavg,prcp\n')
    for i in range(len(averages)):
        formatted_averages = str(averages.iloc[i].to_list()).strip('[]').replace(' ', '').replace('nan', '')
        formatted_prcp_flags = str(prcp_flags.iloc[i].to_list()).strip('[]').lower()
        outfile.write(averages.index[i] + ',' + formatted_averages + ',' + formatted_prcp_flags  + '\n')

