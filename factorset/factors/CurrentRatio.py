# -*- coding:utf-8 -*-
"""
@author:code37
@file:CurrentRatio.py
@time:2018/3/1615:22
"""

import pandas as pd
import tushare as ts
from factorset.factors import BaseFactor
from factorset.data.OtherData import code_to_symbol, shift_date
from factorset.data import CSVParser as cp
from factorset.Util.finance import ttmContinues, ttmDiscrete


class CurrentRatio(BaseFactor):
    """
    :名称: 流动比率（Current Ratio）；营运资金比率（Working Capital Ratio）；真实比率（Real Ratio）
    :计算方法: 流动比率 = 流动资产合计_最新财报 / 流动负债合计_最新财报
    :应用: 流动比率越高，说明资产的流动性越大，短期偿债能力越强。
    """
    def __init__(self, factor_name='CurrentRatio', tickers='000016.SH', data_source='', factor_parameters={}, save_dir=None):
        # Initialize super class.
        super().__init__(factor_name=factor_name, tickers=tickers,
                         factor_parameters=factor_parameters,
                         data_source=data_source, save_dir=save_dir)

    def prepare_data(self,  begin_date, end_date):
        shifted_begin_date = shift_date(begin_date, 500)
        bs = cp.concat_fund(self.data_source, self.tickers, 'BS').loc[shifted_begin_date:end_date,
                    ['ticker', 101, 103]]
        bs['CurrentRatio'] = bs[101] / bs[103]
        self.bs = bs.drop([101, 103], axis=1)
        # bs['report_date'] = bs.index
        # bs['release_date'] = bs.index

    def generate_factor(self, date_str):
        balance_df = self.bs[:date_str]
        balance_df = balance_df.dropna()
        result_se = balance_df.groupby('ticker').apply(lambda x: x['CurrentRatio'].iloc[-1])
        return result_se

if __name__ == '__main__':
    # 设定要需要生成的因子数据范围
    from_dt = '2017-06-15'
    to_dt = '2018-03-09'

    # 取沪深300
    hs300 = ts.get_hs300s()
    hs300.code = hs300.code.apply(code_to_symbol)

    # 实例化因子
    CurrentRatio = CurrentRatio(
        factor_name='CurrentRatio',
        factor_parameters={},
        tickers=hs300.code.tolist(),
        save_dir='',
        data_source='D:\\idwzx\\project\\factorset\\data',
    )

    # 生成因子数据并入库
    CurrentRatio.generate_factor_and_store(from_dt, to_dt)
    print('因子构建完成，并已成功入库!')
