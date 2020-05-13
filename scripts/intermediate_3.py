'''Intermediate BQ 3.

Usage:
  intermediate_3 [--cluster-ip=<cluster-ip>]
  intermediate_3 -h | --help
  intermediate_3 -v | --version

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
import datetime
from os import getenv, environ
from math import isnan
from models import *

def stock_vs_homeless_complaints(rows):
    homeless_2008, homeless_other_yrs, homeless_2008_days, homeless_other_days = 0,0,0,0
    num_pos_days, num_neg_days, negative_days, positive_days = 0,0,0,0

    for item in rows:
        row_date = item['system.castastimestamp(date)']
        # if row_date.year == 2008:
        #     print("2008 homeless ", item['homeless_complaints'])
        #     homeless_2008 += item['homeless_complaints']
        #     homeless_2008_days += 1
        # elif row_date.year != 2008:
        #     print("not 2008 homeless", item['homeless_complaints'])
        #     homeless_other_yrs += item['homeless_complaints']
        #     homeless_other_days += 1
        if item['stock_deviation'] < 0:
            negative_days += item['homeless_complaints']
            num_neg_days += 1
        if item['stock_deviation'] > 0:
            positive_days += item['homeless_complaints']
            num_pos_days += 1

    avg_pos = positive_days / num_pos_days
    avg_neg = negative_days / num_neg_days
    print("Stocks went up this many days ", num_pos_days, " and down this many days ", num_neg_days)
    return (avg_pos,avg_neg)

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

    rows = session.execute('SELECT CAST(date AS timestamp), stock_deviation, homeless_complaints FROM spardata.stock_weather311', timeout=None).all()
    (avg_pos,avg_neg) = stock_vs_homeless_complaints(rows)

    # print("The average number of homelessness complaints in 2008 was %f" % avg_2008)
    # print("The average number of homelessness complaints in other years was %f (2006-07 & 2009-2018)" % avg_other)
    print("The average number of homelessness complaints in days the stocks went up was %f" % avg_pos)
    print("The average number of homelessness complaints in days the stocks went down was %f" % avg_neg)
