# -*- coding:utf-8 -*-
"""
@author:code37
@file:OtherData.py
@time:2018/4/259:39
"""

from time import sleep
from factorset.Util.configutil import GetConfig
import math
import tushare as ts
import pandas as pd
import os
import datetime
import time

############ SETTING #############
config = GetConfig()
MONGO = config.MONGO
CSV = config.CSV
SFL = config.SFL
other_dir = config.other_dir
# MONGO = False
# CSV = True
# SFL = True  # succ and fail list of date

############ CHANGE ABOVE SETTING #############


def write_list(list, url):
    with open(url, 'w') as f:
        f.write(str(list))


def write_all_date(tc, lib=None):
    """
    :param tc: List，所有日期
    :param lib: arctic.store.version_store.VersionStore

    :return: succ: List, written stocks; fail: List, failed written stocks  
    """
    succ = []
    fail = []
    if not os.path.exists(other_dir):
        os.mkdir(other_dir)
    l = []

    for date in tc:
        try:
            df = ts.get_day_all(date)
            df['date'] = date
            l.append(df)
            succ.append(date)
            print(date + '写入完成')
        except Exception as e:
            fail.append(date)
            print("Failed for ", date, str(e))
        sleep(0.2)

    df = pd.concat(l, ignore_index=True)
    df.set_index('date', inplace=True)

    if MONGO and lib:
        lib.write('other', df, metadata={'source': 'Tushare'})
    if CSV:
        df.to_csv(os.path.abspath("{}/{}.csv".format(other_dir, 'other')))

    if SFL:
        write_list(succ, os.path.abspath('./{}/other_succ_list.txt').format(other_dir))
        write_list(fail, os.path.abspath('./{}/other_fail_list.txt').format(other_dir))

def write_new_stocks():
    # 最多取到2016-04-26
    ts.new_stocks(pause=0.1).to_csv(os.path.abspath("{}/{}.csv".format(other_dir, 'newstock')))

def tradecal(startday=None, endday=None):
    if not startday:
        start = '2017-06-15'
    # elif time.strptime(startday,'%Y-%m-%d') < time.strptime('2017-06-15', '%Y-%m-%d'):
    #     start = '2017-06-15'
    elif time.strptime(startday,'%Y-%m-%d') < time.strptime('1990-12-19', '%Y-%m-%d'):
        start = '1990-12-19'
    else:
        start = startday
    if endday:
        end = endday
    else:
        end = datetime.datetime.now().date().strftime('%Y-%m-%d')
    tc = ts.util.dateu.trade_cal()
    tc = tc[tc.isOpen == 1]
    tc.set_index('calendarDate', inplace=True)
    return tc.loc[start:end].index.tolist()

def market_value(dir, tickers):
    df = pd.read_csv(dir, encoding='gbk')[['date', 'code', 'price', 'totals']]
    df.code = df.code.apply(code_to_symbol)
    df.set_index('code', inplace=True)
    if tickers:
        df = df.loc[tickers]
    df.loc[df.totals < 5000, 'totals'] = df.loc[df.totals < 5000, 'totals'] * 10000
    df['mkt_value'] = df.price * df.totals
    df['ticker'] = df.index
    df.set_index('date', inplace=True)
    return df

def code_to_symbol(code):
    '''
        生成symbol代码标志
    '''
    if isinstance(code, int):
        code = str(code)
    if len(str(code)) != 6 :
        return '%s.SZ'%((6-len(code)) * '0' + code)
    else:
        return '%s.SH'%code if code[:1] in ['5', '6', '9'] else '%s.SZ'%code


def shift_date(date_str, n):
    """日期平移函数，获取date_str向后移动N个交易日所对应的交易日

    Args:
        date_str: 日期, "YYYYMMDD"格式的字符串
        n: 时间跨度, int
        direction: 方向, backward

    Returns:
         移动后的date
    """
    tc = ts.util.dateu.trade_cal()
    tc = tc[tc.isOpen == 1]
    tc.set_index('calendarDate', inplace=True)
    if len(tc[:date_str]) < n:
        return tc.iloc[0].name
    else:
        return tc[:date_str].iloc[-500].name

if __name__ == '__main__':
    # mongod --dbpath D:/idwzx/project/arctic
    # a = Arctic('localhost')
    # a.initialize_library('ashare_other')
    # lib = a['ashare_other']
    # write_all_date(tradecal())
    write_new_stocks()