'''Simple Queries.

Usage:
  simple_queries <cluster-ip>
  simple_queries -h | --help
  simple_queries -v | --version

Options:
  -h --help                     Show this screen
  -v --version                  Show version
'''

from docopt import docopt

from cassandra.cluster import Cluster, ExecutionProfile
from cassandra.policies import WhiteListRoundRobinPolicy
from cassandra.cqlengine import connection
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement, dict_factory

import logging
from collections import namedtuple

if __name__ == '__main__':
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.WARNING)
    ARGS = docopt(__doc__, version='Simple Queries 0.1')

    node1_profile = ExecutionProfile(
        load_balancing_policy=WhiteListRoundRobinPolicy([ARGS['<cluster-ip>']]),
        row_factory=dict_factory,
    )
    profiles = {'node1': node1_profile}

    user_info = {'username':'spardata_user', 'password':'inputallthedata'}
    auth_provider = PlainTextAuthProvider(username=user_info['username'], password=user_info['password'])
    logger.info(f'Logging into {ARGS["<cluster-ip>"]} with user={user_info["username"]}')
    session = Cluster(
        [ARGS['<cluster-ip>']],
        port=9042,
        protocol_version=4,
        execution_profiles=profiles,
        connect_timeout=20,
        auth_provider=auth_provider
    ).connect()

    connection.register_connection('cluster1', session=session)
    logger.info(session.execute('SELECT release_version FROM system.local').one())

    print('Simple Queries:')

    print('\n' + 'Average temperature in NYC from 2006-2020:')
    statement = SimpleStatement('SELECT AVG(tavg) as avg_temp FROM spardata.weather_dataset')
    for row in session.execute(statement,timeout=30):
        print(row['avg_temp'])

    print('\n' + 'Average temperature in NYC by year:')
    statement = SimpleStatement('SELECT year, AVG(tavg) as avg_temp FROM spardata.weather_dataset GROUP BY year')
    for row in session.execute(statement,timeout=30):
        print(f"{row['year']} : {row['avg_temp']}")

    print('\n' + 'Average temperature in NYC by year and month:')
    statement = SimpleStatement('SELECT year, month, AVG(tavg) as avg_temp FROM spardata.weather_dataset GROUP BY year, month')
    for row in session.execute(statement, timeout=30):
        print(f"{row['month']}/{row['year']} : {row['avg_temp']}")

    # print('\n' + 'Average stock price by exchange:')
    # statement = SimpleStatement('SELECT exchange, AVG(avg) as avg_stock_price FROM spardata.stock_dataset GROUP BY exchange')
    # for row in session.execute(statement, timeout=30):
    #    print(f"{row['exchange']} : {row['avg_stock_price']}")

    # print('\n' + 'Maximum stock price by symbol and exchange:')
    # statement = SimpleStatement('SELECT exchange, symbol, MAX(high) as max_stock_price FROM spardata.stock_dataset GROUP BY exchange, symbol')
    # for row in session.execute(statement, timeout=30):
    #    print(f"{row['symbol']} on {row['exchange']} : {row['avg_stock_price']}")

    print('\n' + 'Maximum amount of 311 complaints in a day:')
    statement = SimpleStatement('SELECT MAX(all_complaints) as max_complaints FROM spardata.stock_weather311')
    for row in session.execute(statement,timeout=30):
        print(row['max_complaints'])

    print('\n' + 'Range of temperatures in NYC by year:')
    statement = SimpleStatement('SELECT year, MAX(tmax) as max_max, MIN(tmin) as min_min FROM spardata.weather_dataset GROUP BY year')
    for row in session.execute(statement, timeout=30):
        rng = row['max_max'] - row['min_min']
        print(f"{row['year']} : {rng}")
