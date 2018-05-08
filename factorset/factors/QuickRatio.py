# -*- coding:utf-8 -*-
"""
@author:code37
@file:QuickRatio.py
@time:2018/3/1615:22
"""

import pandas as pd
import tushare as ts
from factorset.factors import BaseFactor
from factorset.data.OtherData import code_to_symbol, shift_date
from factorset.data import CSVParser as cp
from factorset.Util.finance import ttmContinues, ttmDiscrete


class QuickRatio(BaseFactor):
    """
    流动资产合计（最新财报）／ 流动负债合计（最新财报）
    """
    def __init__(self, factor_name='QuickRatio', tickers='000016.SH', data_source='', factor_parameters={}, save_dir=None):
        # Initialize super class.
        super().__init__(factor_name=factor_name, tickers=tickers,
                         factor_parameters=factor_parameters,
                         data_source=data_source, save_dir=save_dir)

    def prepare_data(self, begin_date, end_date):
        shifted_begin_date = shift_date(begin_date, 500)
        bs = cp.concat_fund(self.data_source, self.tickers, 'BS').loc[shifted_begin_date:end_date,
                    ['ticker', 101, 52, 139, 88, 103]]
        # 速动资产=流动资产103-存货52=流动资产103-存货52-预付账款139-待摊费用88
        # 流动负债101
        bs['Quick'] = (bs[103] - bs[88] - bs[52] - bs[139]) / bs[101]
        self.balance_sheet = bs.drop([101, 52, 139, 88, 103], axis=1)

    def generate_factor(self, date_str):
        balance_df = self.balance_sheet[:date_str]
        balance_df = balance_df.dropna()
        result_se = balance_df.groupby('ticker').apply(lambda x: x['Quick'].iloc[-1])
        return result_se


if __name__ == '__main__':
    # 设定要需要生成的因子数据范围
    from_dt = '2017-06-15'
    to_dt = '2018-03-09'

    # 取沪深300
    hs300 = ts.get_hs300s()
    hs300.code = hs300.code.apply(code_to_symbol)

    # 实例化因子
    QuickRatio = QuickRatio(
        factor_name='QuickRatio',
        factor_parameters={},
        tickers=hs300.code.tolist(),
        save_dir='',
        data_source='D:\\idwzx\\project\\factorset\\data',
    )

    # 生成因子数据并入库
    QuickRatio.generate_factor_and_store(from_dt, to_dt)
    print('因子构建完成，并已成功入库!')
