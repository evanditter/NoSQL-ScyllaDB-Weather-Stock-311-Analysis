## NoSQL ScyllaDB Project

### What was the purpose of this project?
The main purpose of this project was to 2-fold. 
  1. To analyze how 311 requests in New York City as well as Weather might effect the stock market and if there is a solid prediction on if the market will go up or down on a given day.
  2. To get familiar with a NoSQL storage technology. ScyllaDB is a column-family storage NoSQL technology based off Cassandra but can be up to 10x more efficient. 


The scripts and dataset outputs are currently in the repo. This analysis was originally hosted in AWS. The ScyllaDB database was hosted in the cloud. For privacy reasons, that is kept out of this repo. It is no longer up due to cost concerns of keeping the AWS instance up.

The scripts were used to pull, cleanse, and filter the datasets. The scripts were connected to the AWS instance. There are also some helper or utility scripts. Lastly, there is a set of analysis scripts that were used to draw conclusion or try and find trends between weather, 311 requests, and stock data. 

The tech stack was ScyllaDB, AWS, Docker, bash, and python. My partner did the AWS and bash scripting where I focused on the python scripting which was working with retrieving, cleansing, and filtering the data and then doing the analysis upon it. I still was learning how AWS worked and how to interact with it to get my scripts running.

To connect to the AWS server, the sld.py file is used. The cleansing scripts were already run on the data, but could be ran again with newly pulled data. The cleansed data is in the datasets folder. The sld script will upload the cleansed data to AWS with the models created in models.py. The intermediate scripts and prediction_weather_vs_stock scripts run the data analysis from the AWS cluster. This can no longer be run, as the AWS instance is down for monetary reasons. 
