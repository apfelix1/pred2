import os
from higgsboom.FuncUtils.DateTime import *
import pandas as pd


tdPeriodList = TradingDays(startDate='20200101', endDate='20201231')
tdPeriodList = [date.replace('-','') for date in tdPeriodList]

df = pd.DataFrame({'date':tdPeriodList})
df['count'] = 0
tradelog = pd.read_csv('./tradelog4.csv',index_col= 0 )
tradelog = tradelog[tradelog['startDate'].astype(str)>='20200101']

for index in range(len(tradelog.index)):
    tradeperiod = TradingDays(startDate = tradelog['startDate'].iloc[index].astype(str),endDate = tradelog['endDate'].iloc[index].astype(str))
    for date in tradeperiod:
        df[df['date'] == date]['count'] +=1

# print(df)
