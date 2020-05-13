'''Intermediate BQ 2.

Usage:
  intermediate_2 [--cluster-ip=<cluster-ip>]
  intermediate_2 -h | --help
  intermediate_2 -v | --version

Options:
  -h --help                     Show this screen
  -v --version                  Show version
  --cluster-ip=<cluster-ip>     Set the ScyllaDB cluster ip [default: 127.0.0.1] - 192.168.99.100 for windows
'''
# a26a99e4c67c411eabeff0267269eb85-865011043.us-east-2.elb.amazonaws.com        amazon cluster ip
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


def stock_vs_noise_complaints(rows):
    noise_increase, noise_decrease, total_noise_complaints, i = 0,0,0,0
    for item in rows:
        total_noise_complaints += item['noise_complaints']
        i += 1

    avg_noise_complaints = total_noise_complaints / i
    for item in rows:
        if item['stock_deviation'] < 0:
            if item['noise_complaints'] > avg_noise_complaints:
                noise_increase += 1
            elif item['noise_complaints'] < avg_noise_complaints:
                noise_decrease += 1

    return (noise_increase, noise_decrease)

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
    # logger.info(f'Logging into {ARGS["--cluster-ip"]} with user={user_info["username"]}')
    cluster = Cluster(
        [ARGS['--cluster-ip']], # 192.168.99.100
        port=9042,
        protocol_version=4,
        execution_profiles=profiles,
        connect_timeout=30,
        auth_provider=auth_provider
    )
    # a26a99e4c67c411eabeff0267269eb85-865011043.us-east-2.elb.amazonaws.com        amazon cluster ip
    session = cluster.connect("spardata")
    # should now be connected to scylladb and run different queries and analyze results
    connection.register_connection('cluster1', session=session)
    logger.info(session.execute('SELECT release_version FROM system.local').one())

    rows = session.execute('SELECT date, stock_deviation, noise_complaints FROM spardata.stock_weather311',timeout=None).all()
    (noise_increase, noise_decrease) = stock_vs_noise_complaints(rows)
    print("The noise complaints increased %d times on days where the stocks decreased" % noise_increase)
    print("The noise complaints decreased %d times on days where the stocks decreased" % noise_decrease)
    print("Thus the percentage of times the noise complaints decreased when the stock prices fell is %f" % (noise_decrease/(noise_decrease + noise_increase)))
