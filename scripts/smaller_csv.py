import pandas as pd

df = pd.read_csv("test_311_Service_requests2.csv", nrows=500)

df.to_csv("test_311_Service_requests.csv", index=False)
