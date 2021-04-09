from higgsboom.MarketData.CFuturesMarketDataUtils import *
import pandas as pd
import numpy as np
from higgsboom.MarketData.CSecurityMarketDataUtils import *
import datetime
import os

secUtils = CSecurityMarketDataUtils('Z:/StockData')
tdPeriodList = TradingDays(startDate='20210101', endDate='20210321')
tdPeriodList = [date.replace('-', '') for date in tdPeriodList]

path = './tradelog10/'


def get_tradelog(name,date):
    return pd.read_csv(path+name+'/'+date+'.csv', index_col=0)

def rmtrade(name,limit=4):
    quant = 0
    blsdf = pd.DataFrame(columns=['date', 'quan t'])
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
                    if index in lduetick:
                        lduetick.remove(index)
                        lduetick.append(-1)
                    if (-1) in lduetick:
                        lduetick[lduetick.index(-1)] = index + 20
                        quant += (daydata['startQ'].iloc[index] - daydata['endQ'].iloc[index]) - 0.00012 * (
                                    daydata['startQ'].iloc[index] + daydata['endQ'].iloc[index]) - 0.001*daydata['startQ'].iloc[index]

                # '''
                if daydata['tradingMethod'].iloc[index] == 'Long':
                    if index in sduetick:
                        sduetick.remove(index)
                        sduetick.append(-1)
                    if (-1) in sduetick:
                        sduetick[sduetick.index(-1)] = index + 20
                        quant += (daydata['endQ'].iloc[index] - daydata['startQ'].iloc[index]) - 0.00012 * (
                                daydata['endQ'].iloc[index] + daydata['startQ'].iloc[index])
# '''
        blsdf = blsdf.append({'date': date, 'quant': quant}, ignore_index=True)
    blsdf.to_csv('./rmbalance/' + name[0:6] + 'rmblslog10-l&s.csv')
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







if __name__ == '__main__':
    # spot_trade_return('510050.SH')
    rmtrade('510050.SH', 4)















