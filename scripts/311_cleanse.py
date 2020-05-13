'''Weather Cleanse Script.

Usage:
  311_cleanse FILE_IN FILE_OUT
  311_cleanse -h | --help

Options:
  -h --help                     Show this screen

311_cleanse takes in an csv file and write to an out_file that is a csv.
a command should thus be:
"python 311_cleanse.py input_file.csv out_file.csv"
'''

from docopt import docopt
import pandas as pd

ARGS = docopt(__doc__)
print(ARGS)

csv = ARGS['FILE_IN']
# only reading columns we are interested in to save time
cols = ['Unique Key', 'Created Date', 'Complaint Type', 'Descriptor']
df = pd.read_csv(csv, usecols=cols)
# df = df[['Unique Key', 'Created Date', 'Complaint Type', 'Descriptor']]
df.columns = ['unique_key', 'date', 'complaint_type', 'descriptor']

# reformatting date to 'YYYY-MM-DD'
df['date'] = df['date'].apply(lambda x: '{}-{}-{}'.format(str(x)[6:10], str(x)[:2], str(x)[3:5]))
df['year'] = df['date'].apply(lambda x: int(str(x)[:4]))
df['month'] = df['date'].apply(lambda x: int(str(x)[5:7]))

out_cols = ['date', 'unique_key','year','month','complaint_type', 'descriptor']
df = df[out_cols]

csv_out = ARGS['FILE_OUT']
period = '_'
index = csv_out.index(period)

i = 2010
while i <= 2020:
    df.loc[df['year'] == i]
    df_temp = df.loc[df['year'] == i]
    str_out = csv_out[:index] + str(i) + csv_out[index:]
    df_temp.to_csv(str_out, index=False, header=True)
    i += 1
