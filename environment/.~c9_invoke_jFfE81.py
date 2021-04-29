
import math
import os.path
import matplotlib.pylab as plt
import pandas as pd

import logging
import boto3
from botocore.exceptions import ClientError

from pypfopt.expected_returns import mean_historical_return
from pypfopt.risk_models import CovarianceShrinkage
from pypfopt.efficient_frontier import EfficientFrontier
from binance.client import Client
from datetime import datetime

from binance.exceptions import BinanceAPIException
from dateutil import parser

symbols = []
def getSymbols(filename):
    with open(filename) as f:
        for line in f:
            symbols.append(line.strip())

getSymbols("mainCoins.txt")


### API

binance_api_key = '1nCzfuyXEKHSEXCE9GEIPE6PU2SZkBMsFMXvMnCHN5moH1e2cAFahUY5HllRJP0Q'    #Enter your own API-key here
binance_api_secret = 'WmvBnTFrUA02dEEgPs67cShymtryDQii9BMmPRe5RIwyckoVBfdIJD80C5XfvC0P' #Enter your own API-secret here

### CONSTANTS
binsizes = {"1m": 1, "5m": 5,"15m": 15, "30m": 30, "1h": 60,  "2h": 120,"4h":240,"6h":720,"12h":720, "1d": 1440}
batch_size = 750

binance_client = Client(api_key=binance_api_key, api_secret=binance_api_secret)


### FUNCTIONS

def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

def minutes_of_new_data(symbol,date, kline_size, data, source):
    if len(data) > 0:  old = parser.parse(data["timestamp"].iloc[-1])
    elif source == "binance": old = datetime.strptime(date, '%d %b %Y')
    if source == "binance": new = pd.to_datetime(binance_client.get_klines(symbol=symbol, interval=kline_size)[-1][0], unit='ms')
    return old, new

def get_all_binance(symbol,date, kline_size, save = False):
    filename = '%s-%s-data.csv' % (symbol, kline_size)
    if os.path.isfile(filename): data_df = pd.read_csv(filename)
    else: data_df = pd.DataFrame()
    oldest_point, newest_point = minutes_of_new_data(symbol, date,kline_size, data_df, source = "binance")
    delta_min = (newest_point - oldest_point).total_seconds()/60
    available_data = math.ceil(delta_min/binsizes[kline_size])
    if oldest_point == datetime.strptime('1 Jan 2017', '%d %b %Y'): print('Downloading all available %s data for %s. Be patient..!' % (kline_size, symbol))
    else: print('Downloading %d minutes of new data available for %s, i.e. %d instances of %s data.' % (delta_min, symbol, available_data, kline_size))
    klines = binance_client.get_historical_klines(symbol, kline_size, oldest_point.strftime("%d %b %Y %H:%M:%S"), newest_point.strftime("%d %b %Y %H:%M:%S"))
    data = pd.DataFrame(klines, columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_av', 'trades', 'tb_base_av', 'tb_quote_av', 'ignore' ])
    data['timestamp'] = pd.to_datetime(data['timestamp'], unit='ms')
    if len(data_df) > 0:
        temp_df = pd.DataFrame(data)
        data_df = data_df.append(temp_df)
    else: data_df = data
    data_df.set_index('timestamp', inplace=True)
    if save: data_df.to_csv('./coinData/'+kline_size+'/'+filename)
    print('All caught up..!')
    return data_df



def run_allocation(date, duration):
    df = pd.DataFrame(columns = symbols)
    for symbol in symbols:
        try:
            data = get_all_binance(symbol,date,duration, save=False)
            df[symbol]= data["close"]

        except BinanceAPIException:
            print("Symbol was : " + symbol)

    df = df.apply(pd.to_numeric)
    df.fillna(0)
    print(df)
    mu = mean_historical_return(df)
    S = CovarianceShrinkage(df).ledoit_wolf()



    ef = EfficientFrontier(mu, S)

    weights = ef.max_sharpe()


    #aws, graphql, table'a ekle


    cleaned_weights = ef.clean_weights()
    print(ef.portfolio_performance(verbose=True))
    lists = sorted(weights.items()) # sorted by key, return a list of tuples
    lists = {x:y for x,y in lists if y>0}


    f = open("Coin"+duration+".txt","w")
    f.write( str(lists) )
    f.close()
    print(weights)
    s3 = boto3.client('s3')
    with open("Coin"+duration+".txt", "rb") as f:
        s3.upload_fileobj(f, "model-predictions", "Coin"+duration+".txt")


import schedule
import time

def taskCoin():
    run_allocation('11 Oct 2021', "1d") #12week=190 inst
    run_allocation('11 Jan 2021', "12h") #12week=190 inst
    run_allocation('12 Mar 2021', "4h") #4week=190 inst
    run_allocation('3 Apr 2021', "1h") #1week=190 inst
    run_allocation('9 Apr 2021', "15m") #2days =190 inst

taskCoin()
schedule.every(15).minutes.do(taskCoin)

while True:
  
    # Checks whether a scheduled task 
    # is pending to run or not
    schedule.run_pending()
    time.sleep(1)

