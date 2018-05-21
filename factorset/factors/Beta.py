# -*- coding:utf-8 -*-
"""
@author:code37
@file:Beta.py
@time:2018/2/2717:56
"""

import pandas as pd
import numpy as np
import tushare as ts
from scipy import stats
from factorset.factors import BaseFactor
from factorset.data.OtherData import code_to_symbol, shift_date
from factorset.data import CSVParser as cp
from factorset.data import StockSaver as sp

class Beta(BaseFactor):
    """
    :名称: Beta系数
    :计算方法: 取最近样本区间，分别计算指定证券日普通收益率Xi和沪深300日普通收益率Yi，OLS回归计算Beta。
    :应用: Beta系数是用来衡量两个时间序列之间关系的统计指标。在金融数据的分析中，Beta用来衡量个股相对于市场的风险。
    """
    def __init__(self, factor_name='Beta_60D', tickers='000016.SH', factor_parameters={'lagTradeDays': 60, 'benchmark': '000300'}, data_source='', save_dir=None):
        # Initialize super class.
        super(Beta, self).__init__(factor_name=factor_name, tickers=tickers,
                                   factor_parameters=factor_parameters,
                                   data_source=data_source, save_dir=save_dir)

        self.lagTradeDays = self.factor_param['lagTradeDays']
        self.benchmark = self.factor_param['benchmark']

    def prepare_data(self, begin_date, end_date):
        """
        数据预处理
        """
        # 多取一些数据做填充
        shifted_begin_date = shift_date(begin_date, self.factor_param['lagTradeDays'])

        # 获取股票行情
        hq = cp.concat_stock(self.data_source, self.tickers).loc[shifted_begin_date:end_date,['code','close']]
        self.hq = cp.hconcat_stock_series(hq, self.tickers)

        # 获取指数Benchmark
        # b = sp.get_index(self.benchmark).loc[shifted_begin_date:end_date,['close']]
        b = pd.read_csv(self.data_source + '\\hq\\' + self.benchmark + '.csv', index_col=0).loc[shifted_begin_date:end_date, ['close']]
        self.b = b.fillna(method='ffill')

    def generate_factor(self, end_day):
        begin_day = shift_date(end_day, self.lagTradeDays)
        close_df = self.hq.loc[begin_day:end_day]
        close_b = self.b.loc[begin_day:end_day]

        ret = close_df / close_df.shift(1) - 1
        bret = close_b / close_b.shift(1) - 1
        bret.dropna(axis=0, how='any', inplace=True)

        # 查找无数据Ticker
        if not len(ret.dropna(axis=1, how='all').columns) == len(ret.columns):
            nonticker = list(set(ret.columns) - set(ret.dropna(axis=1, how='all').columns))
        else:
            nonticker = []

        beta = []
        for columns in ret:
            if columns in nonticker:
                beta.append(np.nan)
            else:
                # 每只股票数据量与Benchmark数据量对应
                retseries = ret[columns].dropna(axis=0, how='any')
                bseries = bret.iloc[len(bret) - len(retseries.index):len(bret)]
                OLSresult = stats.linregress(retseries.values, bseries.values.flatten())
                try:
                    beta.append(OLSresult[0])
                except IndexError:
                    beta.append(np.nan)

        beta_df = pd.Series(beta, index=ret.columns)

        return beta_df

if __name__ == '__main__':

    # 设定要需要生成的因子数据范围
    # 最多取到6-30
    from_dt = '2017-06-30'
    to_dt = '2018-04-20'

    # 取沪深300
    hs300 = ts.get_hs300s()
    hs300.code = hs300.code.apply(code_to_symbol)

    # 实例化因子
    Beta_60D = Beta(
        factor_name='Beta_60D',
        factor_parameters={'lagTradeDays': 60, 'benchmark': '000300'},
        tickers=hs300.code.tolist(),
        save_dir='',
        data_source='D:\\idwzx\\project\\factorset\\data',
    )

    # 生成因子数据并入库
    Beta_60D.generate_factor_and_store(from_dt, to_dt)
    print('因子构建完成，并已成功入库!')
