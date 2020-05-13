'''Intermediate BQ 1.

Usage:
  intermediate_1 [--cluster-ip=<cluster-ip>]
  intermediate_1 -h | --help
  intermediate_1 -v | --version

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

def stock_vs_prcp(rows):
    prcp_total,no_prcp_total,prcp_count,no_prcp_count = 0.0,0.0,0,0
    for item in rows:
        if item['prcp']:
            # print("in prcp " + str(item['prcp']) +  ' ' + str(float(item['stock_deviation'])))
            prcp_total += float(item['stock_deviation'])
            prcp_count += 1
        elif not item['prcp']:
            # print("in no  prcp " + str(item['prcp']) + ' '+ str(float(item['stock_deviation'])))
            no_prcp_total += float(item['stock_deviation'])
            no_prcp_count += 1
    # print(prcp_total, no_prcp_total, (prcp_total/prcp_count), (no_prcp_total/no_prcp_count), prcp_count,no_prcp_count)
    return (prcp_total/prcp_count), (no_prcp_total/no_prcp_count)

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
    # logger.info(f"Logging into {ARGS['<cluster-ip>']} with user={user_info['username']}")
    cluster = Cluster(
        [ARGS['--cluster-ip']], # 192.168.99.100
        port=9042,
        protocol_version=4,
        execution_profiles=profiles,
        connect_timeout=30,
        auth_provider=auth_provider,
        control_connection_timeout=None
    )
    # a26a99e4c67c411eabeff0267269eb85-865011043.us-east-2.elb.amazonaws.com        amazon cluster ip
    session = cluster.connect("spardata")
    # should now be connected to scylladb and run different queries and analyze results
    connection.register_connection('cluster1', session=session)
    logger.info(session.execute('SELECT release_version FROM system.local').one())

    rows = session.execute('SELECT date, prcp, stock_deviation FROM spardata.stock_weather311',timeout=None).all()

    prcp_deviation, no_prcp_deviation = stock_vs_prcp(rows)
    print('The average deviation on days where it does rain is %f' % prcp_deviation)
    print('The average deviation on days where it does NOT rain is %f' % no_prcp_deviation)
    print(len(rows))
