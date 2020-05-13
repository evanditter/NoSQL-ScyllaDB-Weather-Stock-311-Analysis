'''Weather Cleanse Script.

Usage:
  stock_formatting FILE_IN FILE_OUT
  stock_formatting -h | --help

Options:
  -h --help                     Show this screen

weather_cleanse takes in an csv file and write to an out_file that is a csv.
a command should thus be:
"python stock_formatting.py input_file.csv out_file.csv"
'''

from docopt import docopt

import pandas as pd
import numpy as np

ARGS = docopt(__doc__)
print(ARGS)

df = pd.read_csv(ARGS['FILE_IN'])
df.columns=['symbol','exchange','avg','date','open','close','high']

result = df[['exchange','symbol','avg','date','open','close','high']]

csv_out = ARGS['FILE_OUT']
period = '.'
index = csv_out.index(period)
str_out_1 = csv_out[:index] + str(1) + csv_out[index:]
str_out_2 = csv_out[:index] + str(2) + csv_out[index:]
str_out_3 = csv_out[:index] + str(3) + csv_out[index:]

df_split = np.array_split(result, 3)

df_split[0].to_csv(str_out_1,index=False)
df_split[1].to_csv(str_out_2,index=False)
df_split[2].to_csv(str_out_3,index=False)
