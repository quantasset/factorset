# -*- coding:utf-8 -*-
"""
@author:code37
@file:EP_LYR.py
@time:2018/3/116:49
"""
import pandas as pd
import tushare as ts
from factorset.factors import BaseFactor
from factorset.data.OtherData import code_to_symbol, shift_date, market_value
from factorset.data import CSVParser as cp



class EP_LYR(BaseFactor):
    """
    净利润（不含少数股东权益）_最新年报/总市值
    """
    def __init__(self, factor_name='EP_LYR', tickers='000016.SH', data_source='', factor_parameters={}, save_dir=None):
        # Initialize super class.
        super(EP_LYR, self).__init__(factor_name=factor_name, tickers=tickers,
                         factor_parameters=factor_parameters,
                         data_source=data_source, save_dir=save_dir)

    def prepare_data(self, begin_date, end_date):
        shifted_begin_date = shift_date(begin_date, 500)
        earings_df = cp.concat_fund(self.data_source, self.tickers, 'IS').loc[shifted_begin_date:end_date,['ticker', 40]]
        earings_df['motherNetProfit'] = earings_df[40]
        earings_df.drop(40, axis=1, inplace=True)
        earings_df['reportDate'] = earings_df.index
        earings_df['reportDate'] = earings_df['reportDate'].apply(lambda x: x.strftime("%Y-%m-%d"))
        # 仅仅取年报, 查找是否reportDate是否以12月31日结尾

        self.earings_df = earings_df[earings_df['reportDate'].str.endswith('12-31')]
        # Tushare的市值数据只有17年-now
        df = market_value(self.data_source+'\\other\\otherdata.csv', self.tickers)
        self.mkt_value = df.drop(['price', 'totals'], axis=1)

    def generate_factor(self, trading_day):
        earings_df = self.earings_df.loc[:trading_day]
        earings_df = earings_df.sort_values(by='reportDate', ascending=False)
        earings_df = earings_df.groupby('ticker').apply(lambda x: x.head(1))  # 取最近1年的财报

        today_mkt_value = self.mkt_value.loc[trading_day]

        ret_df = earings_df.merge(today_mkt_value, on='ticker', how='inner')
        ret_df['EP_LYR'] = ret_df['motherNetProfit'] / (ret_df['mkt_value'])
        return ret_df.set_index('ticker')['EP_LYR']


if __name__ == '__main__':
    from_dt = '2017-07-15'
    to_dt = '2018-04-09'

    # 取沪深300
    hs300 = ts.get_hs300s()
    hs300.code = hs300.code.apply(code_to_symbol)

    EP_LYR = EP_LYR(
        factor_name='EP_LYR',
        factor_parameters={},
        tickers=hs300.code.tolist(),
        save_dir='',
        data_source='D:\\idwzx\\project\\factorset\\data',
    )

    EP_LYR.generate_factor_and_store(from_dt, to_dt)
    print('因子构建完成，并已成功入库!')
