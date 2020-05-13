import pandas as pd

df = pd.read_csv('aggregated_symbols.csv')
means = df.groupby('date').mean()
deviations = means['close'] - means['open']

with open('stock_deviations_by_day.csv', 'w') as outfile:
    outfile.write('date,stock_deviation\n')
    for i in range(len(means)):
        outfile.write(means.iloc[i].name + ',' + str(deviations[i]) + '\n')
