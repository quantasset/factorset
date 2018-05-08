# -*- coding:utf-8 -*-
"""
@author:code37
@file:GPOA.py
@time:2018/3/2714:26
"""
import pandas as pd
import tushare as ts
from factorset.factors import BaseFactor
from factorset.data.OtherData import code_to_symbol, shift_date, market_value
from factorset.data import CSVParser as cp
from factorset.Util.finance import ttmContinues,ttmDiscrete


class GPOA(BaseFactor):
    """ Gross Profit On Asset """
    def __init__(self, factor_name='GPOA', tickers='000016.SH', data_source='', factor_parameters={}, save_dir=None):
        # Initialize super class.
        super(GPOA, self).__init__(factor_name=factor_name, tickers=tickers,
                                     factor_parameters=factor_parameters,
                                     data_source=data_source, save_dir=save_dir)

    def prepare_data(self, begin_date, end_date):
        shifted_begin_date = shift_date(begin_date, 700)
        # totalAssets 121
        bs = cp.concat_fund(self.data_source, self.tickers, 'BS').loc[shifted_begin_date:end_date, ['ticker', 121]]
        bs['release_date'] = bs.index
        bs['report_date'] = bs.index
        bs['totalAssets'] = bs[121]
        bs.drop(121, axis=1, inplace=True)

        # revenue 0, cost 4
        inst = cp.concat_fund(self.data_source, self.tickers, 'IS').loc[shifted_begin_date:end_date, ['ticker', 0, 4]]
        inst['release_date'] = inst.index
        inst['report_date'] = inst.index
        inst['revenue'] = inst[0]
        inst['cost'] = inst[4]
        inst.drop([0, 4], axis=1, inplace=True)

        totalAssetsTTM_ls = []
        revenueTTM_ls = []
        for ticker in inst['ticker'].unique():
            try:  # 财务数据不足4条会有异常
                reven_df = ttmContinues(inst[inst['ticker'] == ticker], 'revenue,cost')
                reven_df['ticker'] = ticker
            except:
                print(ticker + ': revenue and cost error')
                continue
            revenueTTM_ls.append(reven_df)

        for ticker in bs['ticker'].unique():
            try:
                total_asset_df = ttmDiscrete(bs[bs['ticker'] == ticker], 'totalAssets')
                total_asset_df['ticker'] = ticker
            except:
                print(ticker + ': total asset error')
                continue
            totalAssetsTTM_ls.append(total_asset_df)

        self.bs_TTM = pd.concat(totalAssetsTTM_ls)
        self.inst_TTM = pd.concat(revenueTTM_ls)

    def generate_factor(self, date_str):
        total_assets_ttm_df = self.bs_TTM[self.bs_TTM['datetime'] <= date_str]
        revenue_cost_df = self.inst_TTM[self.inst_TTM['datetime'] <= date_str]

        revenue_cost_df['factor'] = revenue_cost_df['revenue_TTM'] - revenue_cost_df['cost_TTM']
        revenue_cost_se = revenue_cost_df.groupby('ticker').apply(lambda x: x['factor'].iloc[-1])

        total_assets_se = total_assets_ttm_df.groupby('ticker').apply(lambda x: x['totalAssets' + '_TTM'].iloc[-1])

        return revenue_cost_se / total_assets_se

if __name__ == '__main__':
    from_dt = '2017-07-15'
    to_dt = '2018-04-09'

    # 取沪深300
    hs300 = ts.get_hs300s()
    hs300.code = hs300.code.apply(code_to_symbol)

    GPOA = GPOA(
        factor_name='GPOA',
        factor_parameters={},
        tickers=hs300.code.tolist(),
        save_dir='',
        data_source='D:\\idwzx\\project\\factorset\\data',
    )

    GPOA.generate_factor_and_store(from_dt, to_dt)
    print('因子构建完成，并已成功入库!')
    