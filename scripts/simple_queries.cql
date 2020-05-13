SELECT AVG(tavg) as avg_temp FROM spardata.weather_dataset;

SELECT year, AVG(tavg) as avg_temp FROM spardata.weather_dataset GROUP BY year;

SELECT year, month, AVG(tavg) as avg_temp FROM spardata.weather_dataset GROUP BY year, month;

SELECT exchange, AVG(avg) as avg_stock_price FROM spardata.stock_dataset GROUP BY exchange;

SELECT exchange, symbol, MAX(high) as max_stock_price FROM spardata.stock_dataset GROUP BY exchange, symbol;

SELECT MAX(all_complaints) as max_complaints FROM spardata.stock_weather311;

SELECT year, MAX(tmax) as max_max, MIN(tmin) as min_min FROM spardata.weather_dataset GROUP BY year;