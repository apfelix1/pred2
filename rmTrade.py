from higgsboom.MarketData.CFuturesMarketDataUtils import *
import pandas as pd
import numpy as np
from higgsboom.MarketData.CSecurityMarketDataUtils import *
import datetime
import os

secUtils = CSecurityMarketDataUtils('Z:/StockData')
tdPeriodList = TradingDays(startDate='20210101', endDate='20210321')
tdPeriodList = [date.replace('-', '') for date in tdPeriodList]




def get_tradelog(name,date):
    return pd.read_csv(path+name+'/'+date+'.csv', index_col=0)

def rmtrade(name,limit=4):
    quant = 0
    blsdf = pd.DataFrame(columns=['date', 'quant'])
    for date in tdPeriodList:
        print(date)
        daydata = get_tradelog(name, date)

        lduetick = [-1] * limit
        sduetick = [-1]*limit

        for index in range(len(daydata.index)):
            if daydata['tradingSignal'].iloc[index] == 0:
                continue
            else:

                if daydata['tradingMethod'].iloc[index] == 'Short':
                    if index in sduetick:
                        sduetick.remove(index)
                        sduetick.append(-1)
                    if (-1) in sduetick:
                        sduetick[sduetick.index(-1)] = index + 40
                        quant += (daydata['startQ'].iloc[index] - daydata['endQ'].iloc[index]) - 0.00012 * (
                                    daydata['startQ'].iloc[index] + daydata['endQ'].iloc[index]) - 0.001*daydata['startQ'].iloc[index]

                # '''
                if daydata['tradingMethod'].iloc[index] == 'Long':
                    if index in lduetick:
                        lduetick.remove(index)
                        lduetick.append(-1)
                    if (-1) in lduetick:
                        lduetick[lduetick.index(-1)] = index + 40
                        quant += (daydata['endQ'].iloc[index] - daydata['startQ'].iloc[index]) - 0.00012 * (
                                daydata['endQ'].iloc[index] + daydata['startQ'].iloc[index])
# '''
        blsdf = blsdf.append({'date': date, 'quant': quant}, ignore_index=True)
    blsdf.to_csv('./adjtest1/' + name[0:6] + 'rmblslog8-l&s.csv')
    return

def spot_trade_return(name):
    quant = 0

    tradelog = pd.DataFrame(columns=['date', 'quant'])

    for date in tdPeriodList:
        print(date)
        rtarr = pd.read_csv('./etf/' + name + '/' + date + '.csv')

        rtarr_pr = rtarr[rtarr['prrate'] > 0.001]
        rtarr_dc = rtarr[rtarr['dcrate'] > 0.001]

        quant += (rtarr_pr['prETF']*rtarr_pr['prrate']).sum()
        print((rtarr_pr['prETF']*rtarr_pr['prrate']).sum())
        quant += (rtarr_dc['dcETF']*rtarr_dc['dcrate']).sum()
        print((rtarr_dc['dcETF']*rtarr_dc['dcrate']).sum())

        tradelog = tradelog.append({'date':date,'quant':quant}, ignore_index=True)

    tradelog.to_csv('./spot/'+name[0:6]+'t2'+'.csv')
    return

def hybrid_return(name, limit,threshold):
    quant = 0
    blsdf = pd.DataFrame(columns=['date', 'quant'])



    for date in tdPeriodList:
        print(date)
        daydata = get_tradelog(name, date)
        rtarr = pd.read_csv('./etf/' + name + '/' + date + '.csv')

        lcanttrade = pd.DataFrame(columns=['start', 'end'])
        scanttrade = pd.DataFrame(columns=['start', 'end'])

        lduetick = [-1] * limit
        sduetick = [-1] * limit

        for index in range(len(daydata.index)):
            if index in sduetick:
                if not (-1) in sduetick:
                    scanttrade.loc[len(scanttrade.index) - 1]['end'] = daydata.loc[index]['TickLabel']
                sduetick.remove(index)
                sduetick.append(-1)

            if index in lduetick:
                if not (-1) in lduetick:
                    lcanttrade.loc[len(lcanttrade.index) - 1]['end'] = daydata.loc[index]['TickLabel']
                lduetick.remove(index)
                lduetick.append(-1)



            if daydata['tradingSignal'].iloc[index] == 0:
                continue
            else:

                if daydata['tradingMethod'].iloc[index] == 'Short':

                    if (-1) in sduetick:
                        sduetick[sduetick.index(-1)] = index + 40
                        quant += (daydata['startQ'].iloc[index] - daydata['endQ'].iloc[index]) - 0.00012 * (
                                daydata['startQ'].iloc[index] + daydata['endQ'].iloc[index]) - 0.001 * \
                                 daydata['startQ'].iloc[index]
                        if not (-1) in sduetick:
                            scanttrade = scanttrade.append({'start': daydata.loc[index]['TickLabel'], 'end': 'TBD'},
                                                           ignore_index=True)

                # '''
                if daydata['tradingMethod'].iloc[index] == 'Long':

                    if (-1) in lduetick:
                        lduetick[lduetick.index(-1)] = index + 40
                        quant += (daydata['endQ'].iloc[index] - daydata['startQ'].iloc[index]) - 0.00012 * (
                                daydata['endQ'].iloc[index] + daydata['startQ'].iloc[index])
                        if not (-1) in lduetick:
                            lcanttrade = lcanttrade.append({'start': daydata.loc[index]['TickLabel'], 'end': 'TBD'},
                                                           ignore_index=True)

                #print(quant)
        #'''
        print(lcanttrade)

        for i in range(len(lcanttrade.index)):
            if i == 0:
                ltraderange = rtarr[rtarr['timetick']<lcanttrade.loc[i]['start']]
            else:
                ltraderange = rtarr[rtarr['timetick'] <lcanttrade.loc[i]['start']]
                ltraderange = ltraderange[ ltraderange['timetick']>lcanttrade.loc[i-1]['end']]

            ltraderange = ltraderange[ltraderange['prrate']>threshold]
            quant += (ltraderange['prETF']*ltraderange['prrate']).sum()

        for i in range(len(scanttrade.index)):
            if i == 0:
                straderange = rtarr[ rtarr['timetick']< scanttrade.loc[i]['start']]
            else:
                straderange =  rtarr[ rtarr['timetick']< scanttrade.loc[i]['start']]
                straderange = straderange[straderange['timetick'] > scanttrade.loc[i-1]['end']]

            straderange = straderange[straderange['dcrate']> threshold]
            quant += (straderange['dcETF']*straderange['dcrate']).sum()
        #'''

        # '''

        blsdf = blsdf.append({'date': date, 'quant': quant}, ignore_index=True)
    blsdf.to_csv('./hybrid/' + name[0:6] + 'hybrid8-l&s.csv')
    return

def adj_rmt(name,limit):
    quant = 0
    blsdf = pd.DataFrame(columns=['date', 'quant'])
    for date in tdPeriodList:
        print(date)
        daydata = get_tradelog(name, date)

        lduetick = [-1] * limit
        sduetick = [-1] * limit

        for index in range(len(daydata.index)):
            if daydata['tradingSignal'].iloc[index] == 0:
                continue
            else:

                if daydata['tradingMethod'].iloc[index] == 'Short':

                    if (-1) in sduetick:
                        sduetick[sduetick.index(-1)] = index + 40
                        quant += (daydata['startQ'].iloc[index] - daydata['endQ'].iloc[index]) - 0.00012 * (
                                daydata['startQ'].iloc[index] + daydata['endQ'].iloc[index]) - 0.001 * \
                                 daydata['startQ'].iloc[index]

                # '''
                if daydata['tradingMethod'].iloc[index] == 'Long':
                    if index in lduetick:
                        lduetick.remove(index)
                        lduetick.append(-1)
                    if (-1) in lduetick:
                        lduetick[lduetick.index(-1)] = index + 40
                        quant += (daydata['endQ'].iloc[index] - daydata['startQ'].iloc[index]) - 0.00012 * (
                                daydata['endQ'].iloc[index] + daydata['startQ'].iloc[index])
        # '''
        blsdf = blsdf.append({'date': date, 'quant': quant}, ignore_index=True)
    blsdf.to_csv('./adjtest1/' + name[0:6] + 'rmblslog8x-l&s.csv')
    return

def adj_spot(name,threshold):
    quant = 0

    tradelog = pd.DataFrame(columns=['date', 'quant'])

    for date in tdPeriodList:
        print(date)
        rtarr = pd.read_csv('./etf/' + name + '/' + date + '.csv')

        rtarr_pr = rtarr[rtarr['prrate'] > threshold]
        rtarr_dc = rtarr[rtarr['dcrate'] > threshold]

        for index in range(len(rtarr.index)):
            if index ==range(len(rtarr.index))[-1]:
                continue
            if not index in rtarr_pr.index:
                if not index in rtarr_dc.index:
                    continue

            if index in rtarr_pr.index:
                indexp = index+1
                while rtarr.loc[indexp]['prETF'].astype(float) ==0:
                    indexp +=1
                quant += (rtarr.loc[indexp]['prETF'].astype(float)-rtarr.loc[index]['prIOPV'].astype(float))-\
                         (rtarr.loc[indexp]['prETF'].astype(float)+rtarr.loc[index]['prIOPV'].astype(float))*(0.00012)

            if index in rtarr_dc.index:
                indexd = index+1
                while rtarr.loc[indexd]['dcIOPV'].astype(float) == 0:
                    indexd +=1
                quant += (-rtarr.loc[index]['dcETF'].astype(float) + rtarr.loc[indexd]['dcIOPV'].astype(float)) - \
                         (rtarr.loc[index]['dcETF'].astype(float) + rtarr.loc[indexd]['dcIOPV'].astype(float)) * (
                             0.00012)-0.001*(rtarr.loc[index + 1]['dcIOPV'].astype(float))
                print((-rtarr.loc[index]['dcETF'].astype(float) + rtarr.loc[index + 1]['dcIOPV'].astype(float)) - \
                         (rtarr.loc[index]['dcETF'].astype(float) + rtarr.loc[index + 1]['dcIOPV'].astype(float)) * (
                             0.00012)-0.001*(rtarr.loc[index + 1]['dcIOPV'].astype(float)))
        print('quant is '+str(quant))

        tradelog = tradelog.append({'date': date, 'quant': quant}, ignore_index=True)

    tradelog.to_csv('./adjspot/' + name[0:6] + 't0' + '.csv')
    return

def backtest1(name,limit,threshold):
    quant = 0
    blsdf = pd.DataFrame(columns=['date', 'quant'])

    for date in tdPeriodList:
        print(date)
        daydata = get_tradelog(name, date)
        rtarr = pd.read_csv('./etf/' + name + '/' + date + '.csv')

        lcanttrade = pd.DataFrame(columns=['start', 'end'])
        scanttrade = pd.DataFrame(columns=['start', 'end'])

        lduetick = [-1] * limit
        sduetick = [-1] * limit

        for index in range(len(daydata.index)):
            if index in sduetick:
                if not (-1) in sduetick:
                    scanttrade.loc[len(scanttrade.index) - 1]['end'] = daydata.loc[index]['TickLabel']
                sduetick.remove(index)
                sduetick.append(-1)

            if index in lduetick:
                if not (-1) in lduetick:
                    lcanttrade.loc[len(lcanttrade.index) - 1]['end'] = daydata.loc[index]['TickLabel']
                lduetick.remove(index)
                lduetick.append(-1)

            if daydata['tradingSignal'].iloc[index] == 0:
                continue
            else:

                if daydata['tradingMethod'].iloc[index] == 'Short':

                    if (-1) in sduetick:
                        sduetick[sduetick.index(-1)] = index + 40
                        quant += (daydata['startQ'].iloc[index] - daydata['endQ'].iloc[index]) - 0.00012 * (
                                daydata['startQ'].iloc[index] + daydata['endQ'].iloc[index]) - 0.001 * \
                                 daydata['startQ'].iloc[index]
                        if not (-1) in sduetick:
                            scanttrade = scanttrade.append({'start': daydata.loc[index]['TickLabel'], 'end': 'TBD'},
                                                           ignore_index=True)

                # '''
                if daydata['tradingMethod'].iloc[index] == 'Long':

                    if (-1) in lduetick:
                        lduetick[lduetick.index(-1)] = index + 40
                        quant += (daydata['endQ'].iloc[index] - daydata['startQ'].iloc[index]) - 0.00012 * (
                                daydata['endQ'].iloc[index] + daydata['startQ'].iloc[index])
                        if not (-1) in lduetick:
                            lcanttrade = lcanttrade.append({'start': daydata.loc[index]['TickLabel'], 'end': 'TBD'},
                                                           ignore_index=True)

                # print(quant)
        '''
        print(lcanttrade)

        for i in range(len(lcanttrade.index)):
            if i == 0:
                ltraderange = rtarr[rtarr['timetick'] < lcanttrade.loc[i]['start']]
            else:
                ltraderange = rtarr[rtarr['timetick'] < lcanttrade.loc[i]['start']]
                ltraderange = ltraderange[ltraderange['timetick'] > lcanttrade.loc[i - 1]['end']]

            ltraderange = ltraderange[ltraderange['prrate'] > threshold]
            quant += (ltraderange['prETF'] * ltraderange['prrate']).sum()

        for i in range(len(scanttrade.index)):
            if i == 0:
                straderange = rtarr[rtarr['timetick'] < scanttrade.loc[i]['start']]
            else:
                straderange = rtarr[rtarr['timetick'] < scanttrade.loc[i]['start']]
                straderange = straderange[straderange['timetick'] > scanttrade.loc[i - 1]['end']]

            straderange = straderange[straderange['dcrate'] > threshold]
            quant += (straderange['dcETF'] * straderange['dcrate']).sum()
         '''

        # '''

        blsdf = blsdf.append({'date': date, 'quant': quant}, ignore_index=True)
    blsdf.to_csv('./backtest1/' + name[0:6] + '1bt8-l&s.csv')
    return

def money_in_use(name):
    blsdf = pd.DataFrame(columns=['date', 'quant'])
    for date in tdPeriodList:
        print(date)
        tradelog = get_tradelog(name,date)
        daymax = []
        sdaymax = tradelog[tradelog['tradingMethod'] == 'Short']
        sum = (sdaymax['startQ'] * sdaymax['tradingSignal']).sum()
        for index in range(len(tradelog)):
            if index < 20:
                continue
            ldaymax = tradelog.loc[index-20:index]
            ldaymax = ldaymax[ldaymax['tradingMethod'] == 'Long']
            daymax.append(sum+(ldaymax['startQ']*ldaymax.loc[index-20:index]['tradingSignal']).sum())

        blsdf = blsdf.append({'date':date,'quant':max(daymax)}, ignore_index=True)

    blsdf.to_csv('./moneyinuse/' + name[0:6] + 'miut2.csv')
    return

#这个tradelog记录在融资情况下加入每天万2.5(年0.08)/1.7(年0.06)的成本
def fin_rmtrade(name):
    quant = 0
    blsdf = pd.DataFrame(columns=['date', 'quant'])
    for date in tdPeriodList:
        print(date)
        daydata = get_tradelog(name, date)

        for index in range(len(daydata.index)):
            if daydata['tradingSignal'].iloc[index] == 1:

                if daydata['tradingMethod'].iloc[index] == 'Short':
                    quant += (daydata['startQ'].iloc[index] - daydata['endQ'].iloc[index]) - 0.00012 * (
                            daydata['startQ'].iloc[index] + daydata['endQ'].iloc[index]) - 0.001 * \
                             daydata['startQ'].iloc[index]-0.00017*(daydata['startQ'].iloc[index])

                # '''
                if daydata['tradingMethod'].iloc[index] == 'Long':
                    quant += (daydata['endQ'].iloc[index] - daydata['startQ'].iloc[index]) - 0.00012 * (
                            daydata['endQ'].iloc[index] + daydata['startQ'].iloc[index])-0.00017*(daydata['startQ'].iloc[index])
                    # '''

        blsdf = blsdf.append({'date': date, 'quant': quant}, ignore_index=True)
    blsdf.to_csv('./finrmt2/' + name[0:6] + 't3.csv')
    return

def adj_spot2(name,threshold,shock):
    quant = 0

    tradelog = pd.DataFrame(columns=['date', 'quant'])

    for date in tdPeriodList:
        print(date)
        rtarr = pd.read_csv('./etf/' + name + '/' + date + '.csv')

        rtarr_pr = rtarr[rtarr['prrate'] > threshold+shock]
        rtarr_dc = rtarr[rtarr['dcrate'] > threshold+shock]

        for index in range(len(rtarr.index)):
            if index ==range(len(rtarr.index))[-1]:
                continue
            if not index in rtarr_pr.index:
                if not index in rtarr_dc.index:
                    continue

            if index in rtarr_pr.index:
                indexp = index+1
                while rtarr.loc[indexp]['prETF'].astype(float) ==0:
                    indexp +=1
                quant += (rtarr.loc[indexp]['prETF'].astype(float)-rtarr.loc[index]['prIOPV'].astype(float))-\
                         (rtarr.loc[indexp]['prETF'].astype(float)+rtarr.loc[index]['prIOPV'].astype(float))*(0.00012)\
                         - shock*(rtarr.loc[indexp]['prETF'].astype(float))

            if index in rtarr_dc.index:
                indexd = index+1
                while rtarr.loc[indexd]['dcIOPV'].astype(float) == 0:
                    indexd +=1
                quant += (-rtarr.loc[index]['dcETF'].astype(float) + rtarr.loc[indexd]['dcIOPV'].astype(float)) - \
                         (rtarr.loc[index]['dcETF'].astype(float) + rtarr.loc[indexd]['dcIOPV'].astype(float)) * (
                             0.00012)-0.001*(rtarr.loc[index + 1]['dcIOPV'].astype(float)) -\
                         shock*(rtarr.loc[index + 1]['dcETF'].astype(float))
                print((-rtarr.loc[index]['dcETF'].astype(float) + rtarr.loc[index + 1]['dcIOPV'].astype(float)) - \
                         (rtarr.loc[index]['dcETF'].astype(float) + rtarr.loc[index + 1]['dcIOPV'].astype(float)) * (
                             0.00012)-0.001*(rtarr.loc[index + 1]['dcIOPV'].astype(float)))
        print('quant is '+str(quant))

        tradelog = tradelog.append({'date': date, 'quant': quant}, ignore_index=True)

    tradelog.to_csv('./adjspot2/' + name[0:6] + 't0' + '.csv')
    return

path = './tradelog18/'


if __name__ == '__main__':
    #rmtrade('510300.SH',4)
    #spot_trade_return('510050.SH')
    #hybrid_return('510300.SH', 4, 0.0001)
    #adj_rmt('510300.SH',4)
    #adj_spot('510300.SH',0.000)
    #backtest1('510050.SH',0.0001)
    #money_in_use('510050.SH')
    #fin_rmtrade('510050.SH')
    adj_spot2('510300.SH',0,0.001)















