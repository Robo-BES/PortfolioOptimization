import pandas as pd
from pypfopt.expected_returns import mean_historical_return
from pypfopt.risk_models import CovarianceShrinkage
from pypfopt.efficient_frontier import EfficientFrontier
import matplotlib.pylab as plt
import requests

import logging
import boto3
from botocore.exceptions import ClientError



df = pd.read_csv('/home/ec2-user/environment/Allianz.csv', index_col=0)
cols = df.columns
print(df)
df[cols] = df[cols].apply(pd.to_numeric, errors='coerce')
df.fillna(0)
mu = mean_historical_return(df)
S = CovarianceShrinkage(df).ledoit_wolf()
ef = EfficientFrontier(mu, S)
weights = ef.max_sharpe()
#aws, graphql, table'a ekle
cleaned_weights = ef.clean_weights()
lists = sorted(cleaned_weights.items()) # sorted by key, return a list of tuples
lists = {x:y for x,y in lists if y>0.02}
print(lists)
f = open("markowitzAllianz.txt","w")
f.write( str(lists) )
f.close()
s3 = boto3.client('s3')
with open("markowitzAllianz.txt", "rb") as f:
    s3.upload_fileobj(f, "model-predictions", "Markowitz_Allianz_MaxSharpe.txt")


url = "https://l495hx664h.execute-api.eu-central-1.amazonaws.com/dev/markowitz"
payload = {"result": lists}
headers = {"Content-Type": "application/json"}
response = requests.request("POST", url, json=payload, headers=headers)
print(response.text)


url = "https://l495hx664h.execute-api.eu-central-1.amazonaws.com/dev/scrape/daily"
codes = ["AEC","AEE","AEN","AEP","AEU","AEZ","AGL","ALH","ALI","ALR","ALS","ALU","AMA","AMB","AMF","AMG","AMP","AMR","AMS","AMY","AMZ","APG","AUA","AUG","AZA","AZB","AZD","AZH","AZK","AZL","AZM","AZN","AZO","AZS","AZT","AZY","FYU","FYY","KOE"]
payload = {"codes": codes}
headers = {"Content-Type": "application/json"}
response = requests.request("POST", url, json=payload, headers=headers)