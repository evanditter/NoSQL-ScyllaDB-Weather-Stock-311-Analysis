from cassandra.cqlengine.models import Model
from cassandra.cqlengine import columns

class ThreeOneOneDataset(Model):
    __keyspace__    = 'spardata'
    __connection__  = 'cluster1'
    date            = columns.Date(primary_key=True, required=True)
    unique_key      = columns.Integer(primary_key=True, required=True)
    year            = columns.Integer(required=False)
    month           = columns.Integer(required=False)
    complaint_type  = columns.Text(required=True)
    descriptor      = columns.Text(required=False)

class WeatherDataset(Model):
    __keyspace__        = 'spardata'
    __connection__      = 'cluster1'
    year                = columns.Integer(primary_key=True, required=True)
    month               = columns.Integer(primary_key=True, required=True)
    date                = columns.Date(primary_key=True, required=True)
    station_location    = columns.Text(primary_key=True, required=True)
    tmin                = columns.Double(required=False, default=None)
    tmax                = columns.Double(required=False, default=None)
    tavg                = columns.Double(required=False, default=None)
    prcp                = columns.Boolean(required=False, default=False)

class StockDataset(Model):
    __keyspace__    = 'spardata'
    __connection__  = 'cluster1'
    exchange        = columns.Text(primary_key=True, required=True)
    symbol          = columns.Text(primary_key=True, required=True)
    avg             = columns.Double(required=True)
    date            = columns.Date(primary_key=True, required=True)
    open            = columns.Double(required=True)
    close           = columns.Double(required=True)
    high            = columns.Double(required=True)

class StockWeather311(Model):
    __keyspace__        = 'spardata'
    __connection__      = 'cluster1'
    date                = columns.Date(primary_key=True, required=True)
    stock_deviation     = columns.Double(required=True)
    prcp                = columns.Boolean(required=True, default=False)
    all_complaints      = columns.Integer(required=False)
    noise_complaints    = columns.Integer(required=False)
    homeless_complaints = columns.Integer(required=False)
    tavg                = columns.Double(required=False, default=None)
    tmax                = columns.Double(required=False, default=None)
    tmin                = columns.Double(required=False, default=None)
