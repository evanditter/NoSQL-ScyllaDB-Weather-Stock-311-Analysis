'''Scylla Load Data.

Usage:
  sld (threeoneone | weather | stock | all) INFILE [--cluster-ip=<cluster-ip>]
  sld -h | --help
  sld -v | --version

Options:
  -h --help                     Show this screen
  -v --version                  Show version
  --cluster-ip=<cluster-ip>     Set the ScyllaDB cluster ip [default: 127.0.0.1] - 192.168.99.100 for windows
'''

from docopt import docopt

from cassandra.cluster import Cluster, ExecutionProfile
from cassandra.policies import WhiteListRoundRobinPolicy
from cassandra.cqlengine.management import sync_table, create_keyspace_simple
from cassandra.cqlengine import connection
from cassandra.auth import PlainTextAuthProvider

import logging
import pandas as pd
from os import getenv, environ
from math import isnan
from models import *

ARGS = {}

def remove_nan(dictionary):
    newDict = {}
    for key,value in dictionary.items():
        if type(value) == float:
            if not isnan(value):
                newDict[key] = value
        else:
            newDict[key] = value
    return newDict

def add_data(model, csv):
    logger.info(f'starting {model.__name__} data load from \'{csv}\'')
    sync_table(model)

    df = pd.read_csv(csv)
    for row in df.itertuples(index=False):
        model.create(**remove_nan(row._asdict()))

    sync_table(model)
    logger.info(f'finished {model.__name__} data load from {csv}')

if __name__ == '__main__':
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO)
    ARGS = docopt(__doc__, version='Scylla Load Data 0.1')

    if getenv('CQLENG_ALLOW_SCHEMA_MANAGEMENT') is None:
        environ['CQLENG_ALLOW_SCHEMA_MANAGEMENT'] = '1'

    node1_profile = ExecutionProfile(load_balancing_policy=WhiteListRoundRobinPolicy([ARGS['--cluster-ip']]))
    profiles = {'node1': node1_profile}

    user_info = {'username':'spardata_user', 'password':'inputallthedata'}
    auth_provider = PlainTextAuthProvider(username=user_info['username'], password=user_info['password'])
    logger.info(f'Logging into {ARGS["--cluster-ip"]} with user={user_info["username"]}')
    session = Cluster(
        [ARGS['--cluster-ip']], # 192.168.99.100
        port=9042,
        protocol_version=4,
        execution_profiles=profiles,
        connect_timeout=20,
        auth_provider=auth_provider
    ).connect()

    connection.register_connection('cluster1', session=session)
    logger.info(session.execute('SELECT release_version FROM system.local').one())

    create_keyspace_simple(name='spardata', replication_factor=1, connections=['cluster1'])

    if ARGS['threeoneone']:
        add_data(ThreeOneOneDataset, ARGS['INFILE'])
    elif ARGS['weather']:
        add_data(WeatherDataset, ARGS['INFILE'])
    elif ARGS['stock']:
        add_data(StockDataset, ARGS['INFILE'])
    elif ARGS['all']:
        add_data(StockWeather311, ARGS['INFILE'])
    else:
        raise Exception
    
