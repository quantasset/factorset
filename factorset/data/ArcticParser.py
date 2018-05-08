# -*- coding:utf-8 -*-
"""
@author:code37
@file:Data_reader.py
@time:2018/4/1816:55
"""

from arctic import Arctic
from factorset.data import FundDict as fd
import pandas as pd
import re

def all_symbol(lib):
    return lib.list_symbols()

def concat_all_stock(lib, date_range=None):
    return pd.concat([lib.read(s, date_range=date_range).data for s in lib.list_symbols()])

def concat_all_fund(lib, type):
    # type = 'BS','IS','CF'
    l = []
    Dict_to_call = getattr(fd, '{}_DICT'.format(type))
    for s in lib.list_symbols():
        df = lib.read(s).data
        df.columns = df.columns.to_series().map(Dict_to_call)
        df.sort_index(axis=1, inplace=True)
        df['ticker'] = s
        l.append(df)
    return pd.concat(l)

def concat_stock(lib, tickers, date_range=None):
    return pd.concat([lib.read(s, date_range=date_range).data for s in tickers])

def concat_fund(tickers, type):
    # type = 'BS','IS','CF'
    l = []
    Dict_to_call = getattr(fd, '{}_DICT'.format(type))
    for s in tickers:
        df = lib.read(s).data
        df.columns = df.columns.to_series().map(Dict_to_call)
        df.sort_index(axis=1, inplace=True)
        df['ticker'] = s
        l.append(df)
    return pd.concat(l)

def forfor(a):
    return [item for sublist in a for item in sublist]

def fund_collist(lib):
    l = []
    for s in lib.list_symbols():
        try:
            l.append(lib.read(s).data.columns.tolist())
        except Exception as e:
            print(s, e)
    l = set(forfor(l))
    return l


def none_data(lib):
    l = []
    for s in lib.list_symbols():
        if len(lib.read(s).data.values) == 0:
            print(s)
            l.append(s)
    return l

def read_data(lib, ticker):
    """
    :param lib: arctic.store.version_store.VersionStore
    :param ticker: 
    :return: 
    """
    # 去除字符，保留前六位编号
    # ticker = re.sub(r'\D', '', ticker)""
    return lib.read(ticker).data

def write_data(lib, symbol, df):
    lib.write(symbol, df)

def list_libraries(Arctic):
    Arctic.list_libraries()

def delete_symbol(lib, symbol):
    lib.delete(symbol)

def delete_lib(Arctic, lib_name):
    Arctic.delete_library(lib_name)

if __name__ == '__main__':
    # mongod --dbpath D:/idwzx/project/arctic
    a = Arctic('localhost')
    a.initialize_library('ashare_BS')
    a.list_libraries()
    lib = a['ashare_BS']
    print(all_symbol(lib), len(all_symbol(lib)))

    # for symbol in all_symbol(lib):
    #     if len(symbol)==6:
    #         print(symbol)
    #         delete_symbol(lib, symbol)

    # delete_lib(a, 'ashare_BBS')
