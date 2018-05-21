# -*- coding:utf-8 -*-
"""
@author:code37
@file:GrossMarginTTM.py
@time:2018/3/2616:23
"""
import pandas as pd
import tushare as ts
from factorset.factors import BaseFactor
from factorset.data.OtherData import code_to_symbol, shift_date, market_value
from factorset.data import CSVParser as cp
from factorset.Util.finance import ttmContinues

class GrossMarginTTM(BaseFactor):
    """
    :名称: 毛利率；销售毛利率
    :计算方法: =（营业收入_TTM - 营业成本_TTM）/ 营业收入_TTM，营业收入_TTM为最近4个季度报告期的营业收入之和，营业成本_TTM为最近4个季度报告期的营业成本之和。
    :应用: 毛利率越高表明企业的盈利能力越强，控制成本的能力越强。但是对于不同规模和行业的企业，毛利率的可比性不强。
    """
    def __init__(self, factor_name='GrossMarginTTM', tickers='000016.SH', data_source='', factor_parameters={}, save_dir=None):
        # Initialize super class.
        super(GrossMarginTTM, self).__init__(factor_name=factor_name, tickers=tickers,
                                   factor_parameters=factor_parameters,
                                   data_source=data_source, save_dir=save_dir)

    def prepare_data(self, begin_date, end_date):
        shifted_begin_date = shift_date(begin_date, 700)
        inst = cp.concat_fund(self.data_source, self.tickers, 'IS').loc[shifted_begin_date:end_date, ['ticker', 0, 4]]
        inst['release_date'] = inst.index
        inst['report_date'] = inst.index
        inst['revenue'] = inst[0]
        inst['cost'] = inst[4]
        inst.drop([0, 4], axis=1, inplace=True)

        revenueTTM_ls = []
        for ticker in inst['ticker'].unique():
            try:  # 财务数据不足4条会有异常
                reven_df = ttmContinues(inst[inst['ticker'] == ticker], 'revenue,cost')
                reven_df['ticker'] = ticker
            except:
                print(ticker + ': revenue and cost error')
                continue
            revenueTTM_ls.append(reven_df)

        self.revenue_cost_TTM = pd.concat(revenueTTM_ls)

    def generate_factor(self, date_str):
        revenue_cost_df = self.revenue_cost_TTM[self.revenue_cost_TTM['datetime'] <= date_str]

        revenue_cost_df['factor'] = (revenue_cost_df['revenue_TTM'] - revenue_cost_df['cost_TTM']) \
                                    / revenue_cost_df['revenue_TTM']
        result_se = revenue_cost_df.groupby('ticker').apply(lambda x: x['factor'].iloc[-1])

        return result_se

if __name__ == '__main__':
    from_dt = '2017-06-30'
    to_dt = '2018-04-09'

    # 取沪深300
    hs300 = ts.get_hs300s()
    hs300.code = hs300.code.apply(code_to_symbol)

    GrossMarginTTM = GrossMarginTTM(
        factor_name='GrossMarginTTM',
        factor_parameters={},
        tickers=hs300.code.tolist(),
        save_dir='',
        data_source='D:\\idwzx\\project\\factorset\\data',
    )

    GrossMarginTTM.generate_factor_and_store(from_dt, to_dt)
    print('因子构建完成，并已成功入库!')
