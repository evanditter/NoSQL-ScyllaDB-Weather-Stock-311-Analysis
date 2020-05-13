'''Weather Cleanse Script.

Usage:
  weather_cleanse FILE_IN FILE_OUT
  weather_cleanse -h | --help

Options:
  -h --help                     Show this screen

weather_cleanse takes in an csv file and write to an out_file that is a csv.
a command should thus be:
"python weather_cleanse.py input_file.csv out_file.csv"
'''

from docopt import docopt

import pandas as pd

ARGS = docopt(__doc__)
print(ARGS)
saved_cols = [1,2,3,4]

station_df = pd.read_fwf('ghcnd-stations.txt')
station_df.columns=["station_id",'long','lat','elevation','location','gsn_flag','hcn_flag','wmo_id']
station_df2 = station_df[['station_id','location']]
NYC_list = []
for row in station_df2.itertuples():
    if 'NEW YORK CNTRL PK TWR' in row.location or 'NEW YORK LAGUARDIA AP' in row.location or 'NEW YORK JFK INTL AP' in row.location:
        NYC_list.append([row.station_id,row.location])

station_df_result = pd.DataFrame(NYC_list, columns=['station_id','location'])

df = pd.read_csv(ARGS['FILE_IN'])
df.columns=['station_id','date','condition','value','E','F','G','H']

df2 = df[['station_id','date','condition','value']]

column_names = ['station_id','date','tmin','tmax','tavg','prcp']
result = pd.DataFrame(columns = column_names)
# df2.to_csv("testy.csv",index=False)

# separate conditions into lists on the values we are interested in
TMIN_list = []
TMAX_list = []
TAVG_list = []
PRCP_list = []
search_lst = []
for lst in NYC_list: # created list to test if in NYC
    for elem in lst:
        search_lst.append(elem)

for row in df2.itertuples():
    if row.station_id in search_lst:
        if row.condition == 'TMIN':
            TMIN_list.append([row.station_id,row.date,(float(row.value)/10)])
        elif row.condition == 'TMAX':
            TMAX_list.append([row.station_id,row.date,(float(row.value)/10)])
        elif row.condition == 'TAVG':
            TAVG_list.append([row.station_id,row.date,(float(row.value)/10)])
        elif (row.condition == 'PRCP' and row.value > 0) or (row.condition == 'SNOW' and row.value > 0) or (row.condition == 'SNWD' and row.value > 0):
            PRCP_list.append([row.station_id,row.date,True])
        elif (row.condition == 'PRCP' and row.value <= 0) or (row.condition == 'SNOW' and row.value <= 0) or (row.condition == 'SNWD' and row.value <= 0):
            PRCP_list.append([row.station_id,row.date,False])

# create a dataframe for each characteristic of weather we are interested in
result1 = pd.DataFrame(TMIN_list, columns=['station_id','date','tmin'])
result2 = pd.DataFrame(TMAX_list, columns=['station_id','date','tmax'])
result3 = pd.DataFrame(TAVG_list, columns=['station_id','date','tavg'])
result4 = pd.DataFrame(PRCP_list, columns=['station_id','date','prcp'])

#merge the datasets on station and date
result = pd.merge(result1,result2, on=['station_id','date'],how='outer')
result = pd.merge(result,result3, on=['station_id','date'],how='outer')
result = pd.merge(result,result4, on=['station_id','date'],how='outer')

result = pd.merge(result,station_df_result, on=['station_id'],how='left')
# format results so date is first and station id is not in the output
result = result[['date','location','station_id','tmin','tmax','tavg','prcp']]
result = result.drop_duplicates(subset =['date','location'], keep = 'first')
del result['station_id']
result.columns = ['date','station_location','tmin','tmax','tavg','prcp']


# Formatting date to 'YYYY-MM-DD'
result['date'] = result['date'].apply(lambda x: '{}-{}-{}'.format(str(x)[:4], str(x)[4:6], str(x)[6:]))
result['year'] = result['date'].apply(lambda x: int(str(x)[:4]))
result['month'] = result['date'].apply(lambda x: int(str(x)[5:7]))

out_cols = ['year','month','date','station_location','tmin','tmax','tavg','prcp']
result = result[out_cols]
# save to csv
result.to_csv(ARGS['FILE_OUT'],index=False)
