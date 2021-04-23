from higgsboom.MarketData.CFuturesMarketDataUtils import *
import pandas as pd
import numpy as np
from higgsboom.MarketData.CSecurityMarketDataUtils import *
import datetime
import os

secUtils = CSecurityMarketDataUtils('Z:/StockData')
tdPeriodList = TradingDays(startDate='20210101', endDate='20210321')
tdPeriodList = [date.replace('-', '') for date in tdPeriodList]

def get_rtarr(name, date):
    return pd.read_csv('./etf/'+name+'/'+date+'.csv')

def get_etf_tick_data(name, date):
    return secUtils.FundTAQDataFrame(name, date)

def get_pred_data(name):
    data = pd.read_csv('./data/'+name+'.csv')
    return data

def get_true_value(name):
    pred = get_pred_data(name)

    for date in tdPeriodList:
        rtarr = get_rtarr(name,date)
        preddata = pred[pred['TradingDate'].astype(str) == date]






