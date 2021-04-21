from higgsboom.MarketData.CFuturesMarketDataUtils import *
import pandas as pd
import numpy as np
from higgsboom.MarketData.CSecurityMarketDataUtils import *
import datetime
import multiprocessing as mp
import os
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

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

def day_analysis(name,date,threshold):
    rtarr = get_rtarr(name, date)
    data = get_pred_data(name)

    pdata = data[data['TradingDate'].astype(str) == date]
    pdata.reset_index(drop=True, inplace=True)
    pdata['tradingMethod'] = 'N/A'
    pdata['startQ'] = 0
    pdata['endQ'] = 0
    pdata['tradingSignal'] = 'N/A'
    pdata['expectedReturn'] = 0
    pdata['realReturn'] = 0
    pdata['Commission'] = 0

    for index in range(len(pdata)):
        tick = pdata['TickLabel'].iloc[index]

        rtarr1 = rtarr[rtarr['timetick']> tick]
        rtarr1.reset_index(drop = True,inplace=True)

        if len(rtarr1.index) <41:
            break

        if pdata['pred_return_rate'].iloc[index].astype(float) > 0:
            startq = rtarr1['prIOPV'].iloc[0].astype(float)
            endq = rtarr1['prETF'].iloc[40].astype(float)


            pdata['tradingMethod'].iloc[index] = 'Long'
            pdata['startQ'].iloc[index] = startq
            pdata['endQ'].iloc[index] = endq
            if startq * endq == 0:
                pdata['tradingSignal'].iloc[index] = 0
                continue
            # determine trade signal

            if rtarr1['prrate'].iloc[0].astype(float) + pdata['pred_return_rate'].iloc[index].astype(float) >= threshold:
                if  rtarr1['prETF'].iloc[0].astype(float)- rtarr1['prIOPV'].iloc[0].astype(float)> 0\
                        and rtarr1['prETF'].iloc[0].astype(float)  != 0 \
                        and rtarr1['prIOPV'].iloc[0].astype(float) != 0:
                    pdata['tradingSignal'].iloc[index] = 1
                else:
                    pdata['tradingSignal'].iloc[index] = 0
            else:
                pdata['tradingSignal'].iloc[index] = 0

            # getting return rate
            if pdata['tradingSignal'].iloc[index] == 1:
                pdata['expectedReturn'].iloc[index] = pdata['pred_return_rate'].iloc[index].astype(float)\
                                                      + rtarr1['prrate'].iloc[0].astype(float)
                pdata['realReturn'].iloc[index] = ((pdata['endQ'].iloc[index]-pdata['startQ'].iloc[index]) -
                                                   (0.00012)*(pdata['endQ'].iloc[index]+pdata['startQ'].iloc[index]))\
                                                / pdata['startQ'].iloc[index]
                pdata['Commission'].iloc[index] = (pdata['startQ'].iloc[index] + pdata['endQ'].iloc[index]) * (0.00012)
        # '''
        else:

            startq = rtarr1['dcIOPV'].iloc[0].astype(float)
            endq = rtarr1['dcETF'].iloc[40].astype(float)

            pdata['tradingMethod'].iloc[index] = 'Short'
            pdata['startQ'].iloc[index] = startq
            pdata['endQ'].iloc[index] = endq

            if startq * endq == 0:
                pdata['tradingSignal'].iloc[index] = 0
                continue

            # determine trade signal

            if rtarr1['dcrate'].iloc[0].astype(float) + abs(pdata['pred_return_rate'].iloc[index].astype(float)) >= threshold:
                if rtarr1['dcIOPV'].iloc[0].astype(float)-  rtarr1['dcETF'].iloc[0].astype(float)> 0 and \
                        rtarr1['dcIOPV'].iloc[0].astype(float) !=0 and  rtarr1['dcETF'].iloc[0].astype(float) !=0  :
                    pdata['tradingSignal'].iloc[index] = 1
                else:
                    pdata['tradingSignal'].iloc[index] = 0
            else:
                pdata['tradingSignal'].iloc[index] = 0

            # get return rates
            if pdata['tradingSignal'].iloc[index] == 1:
                pdata['expectedReturn'].iloc[index] = abs(pdata['pred_return_rate'].iloc[index].astype(float))\
                                                      + rtarr1['dcrate'].iloc[0].astype(float)
                pdata['realReturn'].iloc[index] = ((pdata['startQ'].iloc[index]-pdata['endQ'].iloc[index]) -
                                                   (pdata['startQ'].iloc[index]+pdata['endQ'].iloc[index]) *(0.00012) -
                                                   0.001*(pdata['startQ'].iloc[index]))\
                                                / pdata['startQ'].iloc[index]
                pdata['Commission'].iloc[index] = (pdata['startQ'].iloc[index]+pdata['endQ'].iloc[index])*(0.00012)+\
                                                  0.001*(pdata['startQ'].iloc[index])
# '''
    threshold = int(threshold *10000)

    if threshold == 0:
        threshold = '0.0'
    elif 0 < threshold < 10:
        threshold = '0.000' + str(threshold)
    elif threshold == 10 or threshold == 20:
        threshold = '0.00' + str(int(threshold / 10))
    else:
        threshold = '0.00' + str(threshold)

    path = './optimallog/'+name+'/'+threshold
    folder = os.path.exists(path)
    if not folder:
        os.makedirs(path)

    pdata.to_csv(path+'/'+date+'.csv')
    return

def rm_sim(name):
    path = './tradelog10/'
    blsdf = pd.DataFrame(columns=['date', 'quant'])
    quant = 0
    for date in tdPeriodList:
        tradelog = pd.read_csv(path+name+'/'+ date+'.csv',index_col= 0 )
        tradelog1 = tradelog[tradelog['tradingSignal'] == 1]
        ltradelog = tradelog1[tradelog1['tradingMethod'] == 'Long']
        stradelog = tradelog1[tradelog1['tradingMethod'] == 'Short']
        '''
        quant += ((ltradelog['endQ']-ltradelog['startQ']).sum()+(stradelog['startQ'] - stradelog['endQ']).sum())\
                 -0.00012*((ltradelog['endQ']+ltradelog['startQ']).sum()+(stradelog['startQ'] + stradelog['endQ']).sum()) \
                 - 0.001*(stradelog['startQ'].sum())
'''
        quant +=(ltradelog['endQ']-ltradelog['startQ']).sum() - 0.00012*(ltradelog['endQ']+ltradelog['startQ']).sum()

        blsdf = blsdf.append({'date' : date, 'quant' : quant}, ignore_index= True)
    blsdf.to_csv('./'+name[0:6]+'blslog10l.csv')

    print('The revenue from 1 unit of ETF creation/redemption is '+ str(quant))
    return blsdf


def financing_analysis(name,date,data,threshold,fincost):
    rtarr = get_rtarr(name, date)

    pdata = data[data['TradingDate'].astype(str) == date]
    pdata.reset_index(drop=True, inplace=True)
    pdata['tradingMethod'] = 'N/A'
    pdata['startQ'] = 0
    pdata['endQ'] = 0
    pdata['tradingSignal'] = 'N/A'
    pdata['expectedReturn'] = 0
    pdata['realReturn'] = 0
    pdata['Commission'] = 0

    for index in range(len(pdata)):
        tick = pdata['TickLabel'].iloc[index]

        rtarr1 = rtarr[rtarr['timetick']> tick]
        rtarr1.reset_index(drop = True,inplace=True)

        if len(rtarr1.index) <41:
            break

        if pdata['pred_return_rate'].iloc[index].astype(float) > 0:
            startq = rtarr1['prIOPV'].iloc[0].astype(float)
            endq = rtarr1['prETF'].iloc[40].astype(float)


            pdata['tradingMethod'].iloc[index] = 'Long'
            pdata['startQ'].iloc[index] = startq
            pdata['endQ'].iloc[index] = endq
            if startq * endq == 0:
                pdata['tradingSignal'].iloc[index] = 0
                continue
            # determine trade signal

            if rtarr1['prrate'].iloc[0].astype(float) + pdata['pred_return_rate'].iloc[index].astype(
                    float) >= (fincost/360) +threshold:
                if rtarr1['prETF'].iloc[0].astype(float)- rtarr1['prIOPV'].iloc[0].astype(float)> 0\
                        and rtarr1['prETF'].iloc[0].astype(float)  != 0 \
                        and rtarr1['prIOPV'].iloc[0].astype(float) != 0:
                    pdata['tradingSignal'].iloc[index] = 1
                else:
                    pdata['tradingSignal'].iloc[index] = 0
            else:
                pdata['tradingSignal'].iloc[index] = 0




            # getting return rate
            if pdata['tradingSignal'].iloc[index] == 1:
                pdata['expectedReturn'].iloc[index] = pdata['pred_return_rate'].iloc[index].astype(float)\
                                                      + rtarr1['prrate'].iloc[0].astype(float)-(fincost/360)
                pdata['realReturn'].iloc[index] = ((pdata['endQ'].iloc[index]-pdata['startQ'].iloc[index]) -
                                                   (0.00012)*(pdata['endQ'].iloc[index]+pdata['startQ'].iloc[index])
                                                   -(fincost/360)*(pdata['startQ'].iloc[index]))\
                                                / pdata['startQ'].iloc[index]
                pdata['Commission'].iloc[index] = (pdata['startQ'].iloc[index] + pdata['endQ'].iloc[index]) * (0.00012)+\
                                                  (fincost/360)*(pdata['startQ'].iloc[index])
        # '''
        else:

            startq = rtarr1['dcIOPV'].iloc[0].astype(float)
            endq = rtarr1['dcETF'].iloc[20].astype(float)

            pdata['tradingMethod'].iloc[index] = 'Short'
            pdata['startQ'].iloc[index] = startq
            pdata['endQ'].iloc[index] = endq

            if startq * endq == 0:
                pdata['tradingSignal'].iloc[index] = 0
                continue

            # determine trade signal

            if rtarr1['dcrate'].iloc[0].astype(float) + abs(
                    pdata['pred_return_rate'].iloc[index].astype(float)) >= (fincost/360)+threshold:
                if rtarr1['dcIOPV'].iloc[0].astype(float)-  rtarr1['dcETF'].iloc[0].astype(float)> 0 and \
                        rtarr1['dcIOPV'].iloc[0].astype(float) !=0 and  rtarr1['dcETF'].iloc[0].astype(float) !=0  :
                    pdata['tradingSignal'].iloc[index] = 1
                else:
                    pdata['tradingSignal'].iloc[index] = 0
            else:
                pdata['tradingSignal'].iloc[index] = 0




            if pdata['tradingSignal'].iloc[index] == 1:
                pdata['expectedReturn'].iloc[index] = abs(pdata['pred_return_rate'].iloc[index].astype(float))\
                                                      + rtarr1['dcrate'].iloc[0].astype(float)-(fincost/360)
                pdata['realReturn'].iloc[index] = ((pdata['startQ'].iloc[index]-pdata['endQ'].iloc[index]) -
                                                   (pdata['startQ'].iloc[index]+pdata['endQ'].iloc[index]) *(0.00012) -
                                                   0.001*(pdata['startQ'].iloc[index])-(fincost/360)*(pdata['startQ'].iloc[index]))\
                                                / pdata['startQ'].iloc[index]
                pdata['Commission'].iloc[index] = (pdata['startQ'].iloc[index]+pdata['endQ'].iloc[index])*(0.00012)+\
                                                  0.001*(pdata['startQ'].iloc[index])+(fincost/360)*(pdata['endQ'].iloc[index])
# '''
    # print(pdata)
    threshold = int(threshold * 10000)

    if threshold == 0:
        threshold = '0.0'
    elif 0 < threshold < 10:
        threshold = '0.000' + str(threshold)
    elif threshold == 10 or threshold == 20:
        threshold = '0.00' + str(int(threshold / 10))
    else:
        threshold = '0.00' + str(threshold)


    path = './finopt/'+name+'/'+str(fincost)+'/'+threshold
    folder = os.path.exists(path)
    if not folder:
        os.makedirs(path)

    pdata.to_csv(path+'/'+date+'.csv')
    return

def analysis_result(name,threshold=0,fincost = 0):
    predict_data = get_pred_data(name)


    pool = mp.Pool(mp.cpu_count())
    anadf = pd.DataFrame(columns = ['date','nSignal', 'totalSignal','nPosReturn','meanReturn'])
    for date in tdPeriodList:
        pool.starmap_async(financing_analysis, [(name,date,predict_data,threshold*0.0001,0.06) for threshold in list(range(21))])

        #pool.starmap_async(day_analysis, [(name, date, threshold*0.0001) for threshold in list(range(21))])



        # financing_analysis(name,date,predict_data,threshold,fincost)



'''
       daydata = pd.read_csv('./tradelog11/'+name+'/'+date+'.csv',index_col= 0)
       anadf = anadf.append({'date':date, 'nSignal':len(daydata[daydata['tradingSignal']==1].index),
                     'totalSignal':len(daydata.index), 'nPosReturn':len(daydata[daydata['realReturn'] > 0].index),
                     'meanReturn':daydata[daydata['realReturn'] != 0]['realReturn'].mean()},ignore_index= True)

       anadf.to_csv('./'+name[0:6]+'testresult11.csv')
'''


#quant = rm_sim(name='510050.SH')
if __name__=='__main__':
    name = '510050.SH'

    predict_data = get_pred_data(name)

    pool = mp.Pool(mp.cpu_count())
    anadf = pd.DataFrame(columns=['date', 'nSignal', 'totalSignal', 'nPosReturn', 'meanReturn'])
    for date in tdPeriodList:
        pool.starmap_async(financing_analysis,
                           [(name, date, predict_data, threshold * 0.0001, 0.06) for threshold in list(range(21))])

    # data = analysis_result('510300.SH')

