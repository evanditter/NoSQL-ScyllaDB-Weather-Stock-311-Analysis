import os
import glob
import pandas as pd

# Get NASDAQ symbols from file
with open('NASDAQ.txt', 'r') as nasdaq_file:
    nasdaq_symbols = nasdaq_file.read().splitlines()
for i, symbol in enumerate(nasdaq_symbols):
    nasdaq_symbols[i] = symbol.split('\t')[0]

# Get NYSE symbols from file
with open('NYSE.txt', 'r') as nyse_file:
    nyse_symbols = nyse_file.read().splitlines()
for i, symbol in enumerate(nyse_symbols):
    nyse_symbols[i] = symbol.split('\t')[0]

# Aggregate individual stock CSVs
with open('aggregated_symbols.csv', 'w') as outfile:
    outfile.write('symbol,exchange,avg,date,open,close,high\n')
    
    for filename in glob.glob(os.path.join('full_history', '*.csv')):
        df = pd.read_csv(filename)

        # Get stock symbol from filename
        # Might need to adjust this depending on your system
        symbol = filename.replace('full_history\\', '').replace('.csv', '')

        # Get exchange
        if symbol in nasdaq_symbols and symbol in nyse_symbols:
            exchange = 'both'
        elif symbol in nasdaq_symbols:
            exchange = 'nasdaq'
        elif symbol in nyse_symbols:
            exchange = 'nyse'
        else:
            exchange = 'none'
            
        # Insert average, symbol and exchange columns
        df.insert(0, 'avg', (df['high']+df['low'])/2)
        df.insert(0, 'exchange', exchange)
        df.insert(0, 'symbol', symbol)

        # Remove volume, low, and adjclose columns
        df = df.drop(columns=['volume', 'low', 'high'])

        # Write to file
        outfile.write(df.to_csv(header=False, index=False, line_terminator='\n'))
