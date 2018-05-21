# -*- coding:utf-8 -*-
"""
@author:code37
@file:item.py
@time:2018/3/279:20
"""
from time import sleep
from factorset.data.OtherData import code_to_symbol
from factorset.Util.configutil import GetConfig
import math
import tushare as ts
import pandas as pd
import re
import os


############ SETTING #############
config = GetConfig()
MONGO = config.MONGO
CSV = config.CSV
SFL = config.SFL
hqdir = config.hq_dir
# MONGO = False
# CSV = True
# SFL = True # succ and fail list of symbol

############ CHANGE ABOVE SETTING #############

if MONGO:
    from arctic import Arctic
    a = Arctic(config.ahost)
    a.initialize_library('Ashare')
    lib_stock = a['Ashare']

def write_list(list, url):
    with open(url, 'w') as f:
        f.write(str(list))

def write_all_stock(allAshare, lib = None):
    """
    :param allAshare: List，所有股票
    :param lib: arctic.store.version_store.VersionStore
    
    :return: succ: List, written stocks; fail: List, failed written stocks  
    """
    succ = []
    fail = []
    cons = ts.get_apis()
    if not os.path.exists(hqdir):
        os.mkdir(hqdir)

    for symbol in allAshare:
        try:
            df = ts.bar(symbol, cons, freq='D', start_date='', end_date='').sort_index(ascending=True)
            if MONGO and lib:
                lib.write(symbol, df, metadata={'source': 'Tushare'})
            if CSV:
                df.to_csv(os.path.abspath("{}/{}.csv".format(hqdir, symbol)))
            print(symbol + '写入完成')
            succ.append(symbol)
        except Exception as e:
            fail.append(symbol)
            print("Failed for ", symbol, str(e))
        sleep(0.1)

    if SFL:
        write_list(succ, os.path.abspath('./hq_succ_list.txt'))
        write_list(fail, os.path.abspath('./hq_fail_list.txt'))

def get_index(symbol):
    cons = ts.get_apis()
    return ts.bar(symbol, cons, freq='D', start_date='', end_date='', asset='INDEX').sort_index(ascending=True)

def save_index(symbol):
    """
    从Tushare取指数行情，000905 中证500，000300 沪深300
    
    :param symbol: 指数的代码
    """
    cons = ts.get_apis()
    df = ts.bar(symbol, cons, freq='D', asset='INDEX').sort_index(ascending=True)
    df.code = str(symbol)
    if not os.path.exists(hqdir):
        os.mkdir(hqdir)
    df.to_csv(os.path.abspath("{}/{}.csv".format(hqdir, symbol)))
    return

if __name__ == '__main__':
    # mongod --dbpath D:/idwzx/project/arctic
    # a = Arctic('localhost')
    # a.initialize_library('ashare')
    # lib = a['ashare']
    allAshare = pd.read_csv(os.path.abspath('./allAShare.csv'))
    allAshare = allAshare['0']

    write_all_stock(allAshare)
    save_index('000905')

