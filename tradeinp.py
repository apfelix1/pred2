from higgsboom.MarketData.CFuturesMarketDataUtils import *
import pandas as pd
import numpy as np
from higgsboom.MarketData.CSecurityMarketDataUtils import *
import datetime
import multiprocessing as mp
import os
import matplotlib.pyplot as plt

secUtils = CSecurityMarketDataUtils('Z:/StockData')
tdPeriodList = TradingDays(startDate='20200101', endDate='20201231')
tdPeriodList = [date.replace('-','') for date in tdPeriodList]

def get_rtarr(name, date):
    return pd.read_csv('./etf/'+name+'/'+date+'.csv')

def get_etf_tick_data(name, date):
    return secUtils.FundTAQDataFrame(name, date)

def get_pred_data(name):
    data = pd.read_csv('./data/'+name+'.csv')
    data['TradingDate'] = data['TradingDate'].str.replace('-','')
    print(data)

def analysis_result(name):
    for date in tdPeriodList:
        rtarr = get_rtarr(name, date)

get_pred_data('510050.SH')
