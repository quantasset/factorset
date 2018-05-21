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
    :名称: 速动比率（Quick Ratio）；酸性测验比率（Acid-test Ratio）
    :计算方法: 速动比率 = 速动资产合计_最新财报 / 流动负债合计_最新财报；速动资产=流动资产-存货=流动资产-存货-预付账款-待摊费用
    :应用: 速动比率是衡量企业流动资产中可以立即变现用于偿还流动负债的能力。速动资产包括货币资金、短期投资、应收票据、应收账款、其他应收款项等，可以在较短时间内变现。
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
