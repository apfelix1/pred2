from higgsboom.MarketData.CFuturesMarketDataUtils import *
import pandas as pd
import numpy as np
from higgsboom.MarketData.CSecurityMarketDataUtils import *
import datetime
import multiprocessing as mp
import os
import matplotlib.pyplot as plt

secUtils = CSecurityMarketDataUtils('Z:/StockData')

def get_tradelist(name,date):

    tradelist = np.genfromtxt('./')

    return


def get_tick_data(name,date):
    return