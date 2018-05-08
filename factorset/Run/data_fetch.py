# -*- coding:utf-8 -*-
"""
@author:code37
@file:data_fetch.py
@time:2018/5/79:13
"""

from factorset.data import StockSaver, OtherData
from factorset.data.FundCrawler import FundCrawler
from factorset.data.OtherData import code_to_symbol
from factorset.Util.configutil import GetConfig
from factorset import data
from factorset.data import Proxy_start
import pandas as pd
import tushare as ts
import time

def data_fetch():
    gc = GetConfig()
    if gc.target == 'all':
        target = pd.read_csv(data.__file__.strip(data.__file__.split('\\')[-1])+'allAShare.csv')
        target = target['0']
    elif gc.target == 'hs300':
        hs300 = ts.get_hs300s()
        hs300.code = hs300.code.apply(code_to_symbol)
        target = hs300.code.tolist()
    else:
        if isinstance(gc.target, str):
            target = gc.target.split(', ')
        assert isinstance(target, list)

    # Arctic Start
    if gc.MONGO:
        from arctic import Arctic
        a = Arctic(gc.ahost)
        a.initialize_library('Ashare')
        lib_stock = a['Ashare']
    else:
        lib_stock = None

    # Stock & index
    StockSaver.write_all_stock(target, lib_stock)
    try:
        StockSaver.save_index('000905')
        time.sleep(0.1)
        StockSaver.save_index('000300')
    except IOError as e:
        print(e)

    # Other data
    OtherData.write_all_date(OtherData.tradecal())
    OtherData.write_new_stocks()

    # Fundamental data
    while 1:
        if len(Proxy_start.get_all_proxy()) >= gc.proxymin:
            a = FundCrawler('BS')
            a.main(target, num=20)
            b = FundCrawler('IS')
            b.main(target, num=20)
            c = FundCrawler('CF')
            c.main(target, num=20)
            break
        else:
            time.sleep(1)
