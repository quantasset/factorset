# -*- coding:utf-8 -*-
"""
@author:code37
@file:CSVParser.py
@time:2018/4/1816:55
"""

import pandas as pd
import os
from factorset.data import FundDict as fd

#####
dir = os.path.abspath('.')
encode = 'gbk'
#####

def forfor(a):
    return [item for sublist in a for item in sublist]

def all_stock_symbol(dir):
    return [symbol.strip('.csv') for symbol in os.listdir(path='{}/hq'.format(dir))]

def read_stock(dir, ticker):
    """
    :param dir: string
    :param ticker: 
    :return: 
    """
    # 去除字符，保留前六位编号
    # ticker = re.sub(r'\D', '', ticker)""
    return pd.read_csv('{}/hq/{}.csv'.format(dir, ticker), encoding=encode, parse_dates=True, index_col=0)

def concat_all_stock(dir):
    return pd.concat([read_stock(dir, s) for s in all_stock_symbol(dir)])

def hconcat_stock_series(hq, tickers):
    l = []
    for ticker in tickers:
        l.append(hq[hq.code == ticker].close.rename(ticker, inplace=True).fillna(method='ffill'))
    return pd.concat(l, axis=1)

def concat_stock(dir, tickers):
    return pd.concat([read_stock(dir, s) for s in tickers])

def all_fund_symbol(dir, type):
    return [symbol.strip('{}_'.format(type)).strip('.csv') for symbol in os.listdir(path='{}/fund'.format(dir)) if '{}_'.format(type) in symbol]

def read_fund(dir, type, ticker):
    return pd.read_csv('{}/fund/{}_{}.csv'.format(dir, type, ticker), encoding=encode, parse_dates=True, index_col=0)

def fund_collist(dir, type):
    l = []
    for s in all_fund_symbol(dir, type):
        try:
            l.append(read_fund(dir, type, s).columns.tolist())
        except Exception as e:
            print(s, e)
    l = set(forfor(l))
    return l

def concat_fund(dir, tickers, type):
    # type = 'BS','IS','CF'
    l = []
    Dict_to_call = getattr(fd, '{}_DICT'.format(type))
    for s in tickers:
        df = read_fund(dir, type, s)
        df.columns = df.columns.to_series().map(Dict_to_call)
        df.sort_index(axis=1, inplace=True)
        df['ticker'] = s
        l.append(df)
    return pd.concat(l)

def dup(l):
    seen = set()
    return [x for x in l if x in seen or seen.add(x)]


if __name__ == '__main__':
    # print(all_symbol('BS'))
    # print(read_stock(dir, '300593.SZ'))
    # print(all_stock_symbol(dir))
    # print(all_fund_symbol(dir, 'BS'))
    # print(read_fund(dir, 'BS', '000001.SZ'))
    # print(read_fund(dir, 'BS', '000001.SZ').columns)
    # print(read_fund(dir, 'BS', '000002.SZ').columns)
    # print(concat_fund(dir, ['000001.SZ'], 'BS').columns.values)
    # print(concat_fund(dir, ['000002.SZ'], 'BS').columns.values)
    # print(dup(concat_fund(dir, ['000001.SZ'], 'BS').columns.values))
    # print(dup(concat_fund(dir, ['000002.SZ'], 'BS').columns.values))
    # print(len(concat_fund(dir, ['000001.SZ'], 'BS').columns.values), len(set(concat_fund(dir, ['000001.SZ'], 'BS').columns.values)))
    # print(len(concat_fund(dir, ['000002.SZ'], 'BS').columns.values), len(set(concat_fund(dir, ['000002.SZ'], 'BS').columns.values)))
    print(concat_fund(dir, ['000001.SZ', '000002.SZ', '000004.SZ'], 'BS'))
