'''Prediction Weather, 311 Requests, and Stocks.

Usage:
  prediction_weather_vs_stock [--cluster-ip=<cluster-ip>]
  prediction_weather_vs_stock -h | --help
  prediction_weather_vs_stock -v | --version

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
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.metrics import confusion_matrix
from sklearn.metrics import roc_auc_score
from sklearn.metrics import roc_curve, auc
from sklearn import linear_model
from sklearn import tree
from sklearn import model_selection
from sklearn.metrics import roc_curve, auc
from sklearn.ensemble import RandomForestClassifier
from statistics import mean
import statsmodels.api as sm
from os import getenv, environ
from math import isnan
from models import *

# Given the weather, can we predict the stock price deviations for the next week? (or vice versa)
def weather_stock_prediction_linear_regression(rows):
    column_names = ['date', 'prcp', 'stock_deviation', 'tavg', 'tmax', 'tmin','previous_15_weather']
    # df = pd.DataFrame(columns = column_names)
    df_lst = []
    cur_avg_lst = []
    for item in rows:
        if len(cur_avg_lst) >= 15:
            cur_avg_lst.pop()
            cur_avg_lst.append(item['tmax'])
            df_lst.append([item['date'],item['prcp'], item['stock_deviation'], item['tavg'], item['tmax'], item['tmin'], mean(cur_avg_lst)])
        else:
            cur_avg_lst.append(item['tmax'])
            df_lst.append([item['date'],item['prcp'], item['stock_deviation'], item['tavg'], item['tmax'], item['tmin'], mean(cur_avg_lst)])
    df_result = pd.DataFrame(df_lst, columns=column_names)
    df_result['stock_pos_neg'] = df_result['stock_deviation'] >= 0
    df_result[['stock_pos_neg']] *= 1
    df_result = df_result.drop(['stock_deviation'], axis = 1)
    print(df_result.head())
    print("\n")
    print("LINEAR REGRESSION & MULTI-LINEAR REGRESSION MODELS")
    # linear regression models
    target = pd.DataFrame(df_result, columns=['stock_pos_neg'])
    # new_data = df_result.select_dtypes(include='float')
    # Linear regression with the max temperature on stock deviation
    X = df_result['tmax']
    y = target['stock_pos_neg']
    model = sm.OLS(y, X).fit()
    predictions = model.predict(X)
    print("\n")
    print("Linear regression off of the daily max temp")
    print(model.summary())
    # Linear regression with the min temperature on stock deviation
    X = df_result['tmin']
    y = target['stock_pos_neg']
    model = sm.OLS(y, X).fit()
    predictions = model.predict(X)
    print("\n")
    print("Linear regression off of the daily min temp")
    print(model.summary())
    # Linear regression with the precipitation on stock deviation
    X = df_result['prcp']
    y = target['stock_pos_neg']
    model = sm.OLS(y, X).fit()
    predictions = model.predict(X)
    print("\n")
    print("Linear regression off of the daily precipitation")
    print(model.summary())
    # Linear regression with the previous 15 days max temperature on stock deviation
    X = df_result['previous_15_weather']
    y = target['stock_pos_neg']
    model = sm.OLS(y, X).fit()
    predictions = model.predict(X)
    print("\n")
    print("Linear regression off of the previous 15 days daily max temp")
    print(model.summary())
    # Linear regression with the min and max temperature on stock deviation
    X = df_result[['tmax','tmin','previous_15_weather']]
    y = target['stock_pos_neg']
    model = sm.OLS(y, X).fit()
    predictions = model.predict(X)
    print("\n")
    print("Multi-Linear regression off of the daily max, min temp, and the previous 15 days max temperatures")
    print(model.summary())
    print("\n")
    # this is for plotting which we can add, just don't want to add files to scylla randomly
    # fpr, tpr, thresholds = roc_curve(y, predictions)
    # roc_auc = auc(fpr, tpr)
    # plt.figure()
    # plt.plot(fpr, tpr, color='darkorange', label='ROC curve (area = %0.2f)' % roc_auc)
    # plt.plot([0, 1], [0, 1], color='navy', linestyle='--')
    # plt.xlim([0.0, 1.0])
    # plt.ylim([0.0, 1.05])
    # plt.xlabel('False Positive Rate')
    # plt.ylabel('True Positive Rate')
    # plt.title('Receiver operating characteristic')
    # plt.legend(loc="lower right")
    # print(plt.show(block=True))
    # print("\n")

def weather_stock_prediction_RF_classification(rows):
    column_names = ['date', 'prcp', 'stock_deviation', 'tavg', 'tmax', 'tmin','previous_15_weather']
    # df = pd.DataFrame(columns = column_names)
    df_lst = []
    cur_avg_lst = []
    for item in rows:
        if len(cur_avg_lst) >= 15:
            cur_avg_lst.pop()
            cur_avg_lst.append(item['tmax'])
            df_lst.append([item['date'],item['prcp'], item['stock_deviation'], item['tavg'], item['tmax'], item['tmin'], mean(cur_avg_lst)])
        else:
            cur_avg_lst.append(item['tmax'])
            df_lst.append([item['date'],item['prcp'], item['stock_deviation'], item['tavg'], item['tmax'], item['tmin'], mean(cur_avg_lst)])
    df = pd.DataFrame(df_lst, columns=column_names)
    df['stock_pos_neg'] = df['stock_deviation'] >= 0
    df_train = df[:2000]
    df_test = df[-200:]
    # unbalanced random forest classifier with 2000 training points and 200 test points
    # The imbalance is 1244 vs 956 for negative deviations vs positive deviations
    x_test = df_test.drop(['date','tavg','stock_deviation','stock_pos_neg'], axis = 1)
    y_test = df_test['stock_pos_neg']
    # Formatting
    x_train = df_train.drop(['date','tavg','stock_deviation', 'stock_pos_neg'], axis = 1)
    y_train = df_train['stock_pos_neg']
    # Model Creation
    model = RandomForestClassifier(n_estimators = 10)
    model.fit(x_train, y_train)
    y_pred = model.predict(x_test)
    conf_mat = confusion_matrix(y_true=y_test, y_pred=y_pred)
    print("RANDOM FOREST CLASSIFICATION PREDICTION of weather data on stock deviations")
    print("Accuracy: ", model.score(x_test, y_test))
    # Code to generate confusion matrix
    cm = confusion_matrix(y_test, y_pred)
    print(cm)
    print("[[TP FP]")
    print(" [FN TN]]")
    print("\n\n")


# Given the weather, can we predict the stock price deviations for the next week? (or vice versa)
def stock_311_prediction_linear_regression(rows):
    column_names = ['date','stock_deviation', 'all_complaints', 'noise_complaints', 'homeless_complaints']
    # df = pd.DataFrame(columns = column_names)
    df_lst = []
    for item in rows:
        df_lst.append([item['date'],item['stock_deviation'], item['all_complaints'], item['noise_complaints'], item['homeless_complaints']])
    df_result = pd.DataFrame(df_lst, columns=column_names)
    df_result['stock_pos_neg'] = df_result['stock_deviation'] >= 0
    df_result[['stock_pos_neg']] *= 1
    df_result = df_result.drop(['stock_deviation'], axis = 1)
    print(df_result.head())
    print("\n")
    print("LINEAR REGRESSION & MULTI-LINEAR REGRESSION MODELS")
    # linear regression models
    target = pd.DataFrame(df_result, columns=['stock_pos_neg'])
    # new_data = df_result.select_dtypes(include='float')
    # Linear regression with all the 311 complaints in a day vs stock deviation
    X = df_result['all_complaints']
    y = target['stock_pos_neg']
    model = sm.OLS(y, X).fit()
    predictions = model.predict(X)
    print("\n")
    print("Linear regression off of all the 311 requests")
    print(model.summary())
    # Linear regression with noise complaints in a day vs stock deviation
    X = df_result['noise_complaints']
    y = target['stock_pos_neg']
    model = sm.OLS(y, X).fit()
    predictions = model.predict(X)
    print("\n")
    print("Linear regression off of noise complaint requeusts")
    print(model.summary())
    # Linear regression with homeless complaints in a day vs stock deviation
    X = df_result['homeless_complaints']
    y = target['stock_pos_neg']
    model = sm.OLS(y, X).fit()
    predictions = model.predict(X)
    print("\n")
    print("Linear regression off of homeless complaint requeusts")
    print(model.summary())
    # Linear regression with all complaints, noise, and homeless on stock deviation
    X = df_result[['all_complaints','noise_complaints','homeless_complaints']]
    y = target['stock_pos_neg']
    model = sm.OLS(y, X).fit()
    predictions = model.predict(X)
    print("\n")
    print("Multi-Linear regression off of the all, noise, and homeless complaints")
    print(model.summary())
    print("\n\n")

def stock_311_weather_prediction_RF_classification(rows):
    column_names = ['date','stock_deviation', 'all_complaints', 'noise_complaints', 'homeless_complaints','tmin','tmax','prcp','previous_15_weather']
    # df = pd.DataFrame(columns = column_names)
    df_lst = []
    cur_avg_lst = []
    for item in rows:
        if len(cur_avg_lst) >= 15:
            cur_avg_lst.pop()
            cur_avg_lst.append(item['tmax'])
            df_lst.append([item['date'],item['stock_deviation'], item['all_complaints'], item['noise_complaints'], item['homeless_complaints'], item['prcp'], item['tmin'], item['tmax'], mean(cur_avg_lst)])
        else:
            cur_avg_lst.append(item['tmax'])
            df_lst.append([item['date'],item['stock_deviation'], item['all_complaints'], item['noise_complaints'], item['homeless_complaints'], item['prcp'], item['tmin'], item['tmax'], mean(cur_avg_lst)])
    df = pd.DataFrame(df_lst, columns=column_names)
    df['stock_pos_neg'] = df['stock_deviation'] >= 0
    # df_result[['stock_pos_neg']] *= 1
    df_train = df[:2000]
    df_test = df[-200:]
    # unbalanced random forest classifier with 2000 training points and 200 test points
    x_test = df_test.drop(['date','stock_deviation','stock_pos_neg'], axis = 1)
    y_test = df_test['stock_pos_neg']
    # Formatting
    x_train = df_train.drop(['date','stock_deviation', 'stock_pos_neg'], axis = 1)
    y_train = df_train['stock_pos_neg']
    # Model Creation
    model = RandomForestClassifier(n_estimators = 10)
    model.fit(x_train, y_train)
    y_pred = model.predict(x_test)
    conf_mat = confusion_matrix(y_true=y_test, y_pred=y_pred)
    print("RANDOM FOREST CLASSIFICATION PREDICTION for 311 and weather data vs stocks")
    print("Accuracy: ", model.score(x_test, y_test))
    # Code to generate confusion matrix
    cm = confusion_matrix(y_test, y_pred)
    print(cm)
    print("[[TP FP]")
    print(" [FN TN]]")


if __name__ == '__main__':
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO)
    ARGS = docopt(__doc__, version='Scylla Load Data 0.1')

    if getenv('CQLENG_ALLOW_SCHEMA_MANAGEMENT') is None:
        environ['CQLENG_ALLOW_SCHEMA_MANAGEMENT'] = '1'
    # connection to scylla server
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
    rows = session.execute('SELECT * FROM spardata.stock_weather311',timeout=None).all()
    weather_stock_prediction_linear_regression(rows)
    weather_stock_prediction_RF_classification(rows)
    stock_311_prediction_linear_regression(rows)
    stock_311_weather_prediction_RF_classification(rows)
