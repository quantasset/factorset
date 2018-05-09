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
import pandas as pd
import tushare as ts
import time
import requests

def get_proxy(url):
    return requests.get("http://{}/get/".format(url)).content

def get_all_proxy(url):
    return requests.get("http://{}/get_all/".format(url)).json()

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
    print("Start Fetching Stock & Index Data!")
    StockSaver.write_all_stock(target, lib_stock)
    try:
        StockSaver.save_index('000905')
        time.sleep(0.1)
        StockSaver.save_index('000300')
    except IOError as e:
        print(e)
    print("Finish Fetching Stock & Index Data!")

    # Other data
    print("Start Fetching Other Data!")
    OtherData.write_all_date(OtherData.tradecal())
    OtherData.write_new_stocks()
    print("Finish Fetching Other Data!")

    # Fundamental data
    while 1:
        print("Start Fetching Fundamental Data!")
        if len(get_all_proxy(gc.proxypool)) >= gc.proxymin:
            a = FundCrawler('BS')
            a.main(target, num=5)
            b = FundCrawler('IS')
            b.main(target, num=5)
            c = FundCrawler('CF')
            c.main(target, num=5)
            print("Finish Fetching Fundamental Data!")

            break
        else:
            print("Proxy pool is not ready!")
            time.sleep(5)



