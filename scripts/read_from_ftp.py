import wget
import gzip

for i in range(1763, 1768):
    url = 'ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/by_year/' \
                          + str(i) + '.csv.gz'
    wget.download(url)
    with gzip.open(str(i) + '.csv.gz', 'r') as infile:
        with open(str(i) + '.csv', 'w') as outfile:
            content = infile.read().decode()
            outfile.write(content)
