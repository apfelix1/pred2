from higgsboom.MarketData.CFuturesMarketDataUtils import *
import pandas as pd
import numpy as np
from higgsboom.MarketData.CSecurityMarketDataUtils import *
import datetime
import os
import multiprocessing as mp

secUtils = CSecurityMarketDataUtils('Z:/StockData')
tdPeriodList = TradingDays(startDate='20210101', endDate='20210321')
tdPeriodList = [date.replace('-', '') for date in tdPeriodList]

tdPeriodList2020 = TradingDays(startDate='20200101',endDate='20201231')
tdPeriodList2020 = [date.replace('-','') for date in tdPeriodList2020]

def get_rtarr(name, date):
    return pd.read_csv('./etf/'+name+'/'+date+'.csv')

def get_2020rtarr(name,date):
    return pd.read_csv('D:/Intern/ETF/spot trade/etfresult/'+name[0:6]+'/'+date+'.csv')

def get_etf_tick_data(name, date):
    return secUtils.FundTAQDataFrame(name, date)

def get_pred_data(name):
    data = pd.read_csv('./data2/'+name+'.csv',index_col=0)
    return data

def get_prcorr_data(name,date):
    return pd.read_csv('./finana/pr'+name+'/'+date+'.csv', index_col=0)

def get_dccorr_data(name,date):
    return pd.read_csv('./finana/dc'+name+'/'+date+'.csv', index_col=0)

def pretf(row,index,rtarr):
    tick = row['TickLabel']
    rtarr1 = rtarr[rtarr['timetick'] > tick]
    rtarr1.reset_index(drop=True, inplace=True)
    rtarr1 = rtarr1[rtarr1.index > index]
    rtarr1.reset_index(drop=True, inplace=True)

    row['prrate'+str(index)] = rtarr1.loc[0]['prrate'].astype(float)
    return row

def dcetf(row,index,rtarr):
    tick = row['TickLabel']
    rtarr1 = rtarr[rtarr['timetick'] > tick]
    rtarr1.reset_index(drop=True, inplace=True)
    rtarr1 = rtarr1[rtarr1.index > index]
    rtarr1.reset_index(drop=True, inplace=True)

    row['dcrate'+str(index)] = rtarr1.loc[0]['dcrate'].astype(float)
    return row

def get_true_value(name,date):
    pred = get_pred_data(name)

    rtarr = get_rtarr(name,date)

    predata = pred[pred['TradingDate'].astype(str) == date]

    predata.drop(predata.index[:50], inplace=True)
    predata.drop(predata.index[-50:], inplace=True)
    predata['diff'] = predata['pred_return_rate'] - predata['label_return_rate']

    for index in range(40):
        predata = predata.apply(dcetf, axis=1, args=(index,rtarr,))
    predata.reset_index(drop=True, inplace=True)
    predata.to_csv('./finana/'+'dc'+name+'/'+date+'.csv')

    print(date+' finish')
    return

def get_prcorr(name):

    corrdf = pd.DataFrame()
    for date in tdPeriodList:
        data = get_prcorr_data(name,date)
        data = data[data['diff'] > 0]
        for index in range(40):
            data = data[data['prrate'+str(index)] != 0]
        corrsrs = pd.Series()
        corrsrs = corrsrs.append(pd.Series(data=[date],index=['date']))
        for index in range(40):
            corr = data['diff'].corr(other=data['prrate' + str(index)])
            corrsrs = corrsrs.append(pd.Series(data=[corr],index=['prrate'+str(index)]))
        corrdf = corrdf.append(corrsrs,ignore_index=True)
    return corrdf

def get_dccorr(name):

    corrdf = pd.DataFrame()
    for date in tdPeriodList:
        data = get_dccorr_data(name,date)
        data = data[data['diff'] < 0]
        for index in range(40):
            data = data[data['dcrate'+str(index)] != 0]
        corrsrs = pd.Series()
        corrsrs = corrsrs.append(pd.Series(data=[date],index=['date']))
        for index in range(40):

            corr = data['diff'].corr(other=data['dcrate' + str(index)])
            corrsrs = corrsrs.append(pd.Series(data=[corr],index=['dcrate'+str(index)]))
        corrdf = corrdf.append(corrsrs,ignore_index=True)
    return corrdf

def spotTickDiff(name,tickdif=0,impact=0,threshold = 0):

    sheet = pd.DataFrame(columns=['date'])

    for date in tdPeriodList2020:
        print(date)
        rtarr = get_2020rtarr(name,date)
        rtarr1 = rtarr[(rtarr['timetick']>'09:33:00')&(rtarr['timetick']<'14:55:00')]
        rtarr1 = rtarr1[(rtarr1['prrate']-impact>0) | (rtarr1['dcrate']-impact>0)]
        rtarr1['date'] = date
        rtarr1['TradingMethod'] = 'TBD'
        rtarr1['amount'] ='TBD'

        rtarr1['TradingMethod'][rtarr1['prrate']>0] = 'pr'
        rtarr1['TradingMethod'][rtarr1['dcrate']>0] = 'dc'
        rtarr1['rate']=rtarr1['rate'] - impact
        rtarr1 = rtarr1[rtarr1['rate']>0]

        def TickChange(row,tickdif):
            tick = row['timetick']
            rtpr = rtarr[(rtarr['timetick'] >= tick) & (rtarr['prETF'] != 0)]
            rtdc = rtarr[(rtarr['timetick'] >= tick) & (rtarr['dcIOPV'] != 0)]


            if row['TradingMethod'] == 'pr':
                if len(rtpr.index) <= tickdif:
                    row['prETF'] = 0
                    row['amount'] = 0
                    row['rate'] = 0
                    return row
                row['prETF'] = float(rtpr.iloc[tickdif]['prETF'])
                row['amount'] = float(row['prETF']-(impact*row['prETF']) - row['prIOPV']-0.00012*(row['prETF']+row['prIOPV']))
                row['rate'] = float(row['amount']/row['prETF']-impact)
            elif row['TradingMethod'] == 'dc':
                if len(rtdc.index) <= tickdif:
                    row['dcIOPV'] = 0
                    row['amount'] = 0
                    row['rate'] = 0
                    return row
                row['dcIOPV'] = float(
                    rtdc.iloc[tickdif]['dcIOPV'])
                row['amount'] = float(row['dcIOPV']-(impact*row['dcIOPV'])-row['dcETF']-0.00012*(row['dcETF']+row['dcIOPV'])-0.001*(row['dcIOPV']))
                row['rate'] = float(row['amount']/row['dcETF'] -impact)
            return row

        rtarr1 = rtarr1.apply(TickChange,args=(tickdif,),axis=1)

        sheet = sheet.append(rtarr1,ignore_index=True)

    return sheet

def get_ratio(name):
    samt =0
    lamt =0
    for date in tdPeriodList:
        data = pd.read_csv('./tradelog9/'+name+'/'+str(date)+'.csv',index_col=0)
        data = data[data['tradingSignal']==1]
        datas = data[data['tradingMethod'] == 'Short']
        datal = data[data['tradingMethod'] == 'Long']
        samt += len(datas.index)
        lamt +=len(datal.index)
    return[samt,lamt]


if __name__== '__main__':
    lst510050 = get_ratio('510050.SH')
    lst510300 = get_ratio('510300.SH')

    #x=spotTickDiff(name,tickdif,impact)









