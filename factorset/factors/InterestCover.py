# -*- coding:utf-8 -*-
"""
@author:code37
@file:InterestCover.py
@time:2018/3/513:39
"""

import pandas as pd
import tushare as ts
from factorset.factors import BaseFactor
from factorset.data.OtherData import code_to_symbol, shift_date
from factorset.data import CSVParser as cp

class InterestCover(BaseFactor):
    """
     利息覆盖率(InterestCover)
        计算方法：EBIT / 利息费用,其中 EBIT=利润总额+净利息费用

        净利息费用=利息支出-利息收入，若未披露财务费用附注，则直接取财务费用值
    """
    def __init__(self, factor_name='InterestCover', tickers='000016.SH', data_source='', factor_parameters={}, save_dir=None):
        # Initialize super class.
        super(InterestCover, self).__init__(factor_name=factor_name, tickers=tickers,
                         factor_parameters=factor_parameters,
                         data_source=data_source, save_dir=save_dir)

    def prepare_data(self, begin_date, end_date):
        shifted_begin_date = shift_date(begin_date, 500)
        # EBIT / 利息费用，其中 EBIT=利润总额34+净利息费用
        # 净利息费用=利息支出-利息收入，若未披露财务费用附注，则直接取财务费用值56
        inst = cp.concat_fund(self.data_source, self.tickers, 'IS').loc[shifted_begin_date:end_date, ['ticker', 34, 56]]
        self.inst = inst[(inst[56] > 1) | (inst[56] < -1)].copy()

        self.inst['interscover'] = (self.inst[34] + self.inst[56]) / self.inst[56]
        self.inst.sort_index(ascending=True, inplace=True)

    def generate_factor(self, date_str):
        revenue_cost_df = self.inst[:date_str]

        interscover = revenue_cost_df.groupby('ticker').apply(lambda x: x['interscover'].iloc[0])
        if not len(interscover):
            print(date_str)
            print(revenue_cost_df)
        return interscover

if __name__ == '__main__':
    from_dt = '2017-03-06'
    to_dt = '2018-03-09'

    # 取沪深300
    hs300 = ts.get_hs300s()
    hs300.code = hs300.code.apply(code_to_symbol)

    InterestCover = InterestCover(
        factor_name='InterestCover',
        factor_parameters={},
        tickers=hs300.code.tolist(),
        save_dir='',
        data_source='D:\\idwzx\\project\\factorset\\data',
    )

    InterestCover.generate_factor_and_store(from_dt, to_dt)
    print('因子构建完成，并已成功入库!')
