# -*- coding:utf-8 -*-
"""
@author:code37
@file:RoeGrowth223_new.py
@time:2018/3/1917:31
"""

import pandas as pd
import tushare as ts
from factorset.factors import BaseFactor
from factorset.data.OtherData import code_to_symbol, shift_date
from factorset.data import CSVParser as cp
from factorset.Util.finance import ttmContinues, ttmDiscrete


class RoeGrowth2(BaseFactor):
    def __init__(self, factor_name='RoeGrowth2', tickers='000016.SH', data_source='', factor_parameters={}, save_dir=None):
        # Initialize super class.
        super(RoeGrowth2, self).__init__(factor_name=factor_name, tickers=tickers,
                                         factor_parameters=factor_parameters,
                                         data_source=data_source, save_dir=save_dir)

    def prepare_data(self, begin_date, end_date):
        shifted_begin_date = shift_date(begin_date, 800)
        bs = cp.concat_fund(self.data_source, self.tickers, 'BS').loc[shifted_begin_date:end_date,['ticker', 86]]
        bs['release_date'] = bs.index
        bs['report_date'] = bs.index
        bs['motherEquity'] = bs[86]

        # 归母权益
        equity_mean = []
        for ticker in bs['ticker'].unique():
            try:
                tmp_equity = ttmDiscrete(bs[bs['ticker'] == ticker], 'motherEquity', 5)
                tmp_equity['ticker'] = ticker
            except:
                continue
            equity_mean.append(tmp_equity)

        equity_mean = pd.concat(equity_mean)

        inst = cp.concat_fund(self.data_source, self.tickers, 'IS').loc[shifted_begin_date:end_date,['ticker', 40]]
        inst['release_date'] = inst.index
        inst['report_date'] = inst.index
        inst['motherNetProfit'] = inst[40]

        # 归母净利润
        net_profit = []
        for ticker in inst['ticker'].unique():
            try:
                tmp_profit = ttmContinues(inst[inst['ticker'] == ticker], 'motherNetProfit')
                tmp_profit['ticker'] = ticker
            except:
                continue
            net_profit.append(tmp_profit)

        net_profit = pd.concat(net_profit)

        equity_mean['report_date'] = equity_mean['report_date'].apply(lambda x: x.strftime("%Y-%m-%d"))
        net_profit['report_date'] = net_profit['report_date'].apply(lambda x: x.strftime("%Y-%m-%d"))

        self.equity_mean = equity_mean.sort_values(by=['report_date', 'datetime'], ascending=[False, False])
        self.net_profit = net_profit.sort_values(by=['report_date', 'datetime'], ascending=[False, False])

    def generate_factor(self, end_day):
        net_profit_df = self.net_profit[self.net_profit['datetime'] <= end_day]
        print(net_profit_df)
        equity_mean_df = self.equity_mean[self.equity_mean['datetime'] <= end_day]

        net_profit_se = net_profit_df.groupby('ticker').apply(lambda x: x['motherNetProfit' + '_TTM'].iloc[0])
        equity_mean_se = equity_mean_df.groupby('ticker').apply(lambda x: x['motherEquity' + '_TTM'].iloc[0])

        roe = net_profit_se / equity_mean_se

        #  去年同期的因子计算
        latest_report_date = net_profit_df['report_date'].head(1).iloc[0]
        last_year_report_date = str(int(latest_report_date[0:4]) - 1) + latest_report_date[4:]

        # 取每个ticker中最新日期的去年财报, 若取不到则会变成NA被剔除
        last_y_net_profit = net_profit_df[net_profit_df.report_date == last_year_report_date].groupby('ticker').apply(
            lambda x: x['motherNetProfit' + '_TTM'].iloc[0])
        last_y_equity = equity_mean_df[equity_mean_df.report_date == last_year_report_date].groupby('ticker').apply(
            lambda x: x['motherEquity' + '_TTM'].iloc[0])
        last_year_roe = last_y_net_profit / last_y_equity
        return roe - last_year_roe


if __name__ == '__main__':
    # 设定要需要生成的因子数据范围
    from_dt = '2017-06-30'
    to_dt = '2018-04-09'

    # 取沪深300
    hs300 = ts.get_hs300s()
    hs300.code = hs300.code.apply(code_to_symbol)

    RoeGrowth2 = RoeGrowth2(
        factor_name='RoeGrowth2',
        factor_parameters={},
        tickers=hs300.code.tolist(),
        save_dir='',
        data_source='D:\\idwzx\\project\\factorset\\data',
    )

    RoeGrowth2.generate_factor_and_store(from_dt, to_dt)
    print('因子构建完成，并已成功入库!')