# -*- coding:utf-8 -*-
"""
@author:code37
@file:AssetTurnover.py
@time:2018/3/217:39
"""

import pandas as pd
import tushare as ts
from factorset.factors import BaseFactor
from factorset.data.OtherData import code_to_symbol, shift_date, market_value
from factorset.data import CSVParser as cp
from factorset.Util.finance import ttmContinues,ttmDiscrete


class AssetTurnover(BaseFactor):
    """
    :名称: 资产周转率
    :计算方法: 营业收入_TTM / 资产总计_TTM，营业收入_TTM为最近4个季度报告期的营业收入之和，资产总计_TTM为最近5个季度报告期总资产的平均值。
    :应用: 资产周转率越高，表明企业总资产周转速度越快。销售能力越强，资产利用效率越高。
    """
    def __init__(self, factor_name='AssetTurnover', tickers='000016.SH', data_source='', factor_parameters={}, save_dir=None):
        # Initialize super class.
        super(AssetTurnover, self).__init__(factor_name=factor_name, tickers=tickers,
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
        inst.drop(0, axis=1, inplace=True)

        revenueTTM_ls = []
        totalAssetsTTM_ls = []
        for ticker in inst['ticker'].unique():
            try:  # 财务数据不足4条会有异常
                reven_df = ttmContinues(inst[inst['ticker'] == ticker], 'revenue')
                reven_df['ticker'] = ticker
            except:
                print(ticker + ': revenue error')
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

        self.revenueTTM = pd.concat(revenueTTM_ls)
        self.totalAssetsTTM = pd.concat(totalAssetsTTM_ls)

    def generate_factor(self, date_str):
        revenue_ttm_df = self.revenueTTM[self.revenueTTM['datetime'] <= date_str]
        total_assets_ttm_df = self.totalAssetsTTM[self.totalAssetsTTM['datetime'] <= date_str]

        # ttm 的值顺序排列，取最底部的数据即为最新的数据
        revenue_se = revenue_ttm_df.groupby('ticker').apply(lambda x: x['revenue' + '_TTM'].iloc[-1])
        total_assets_se = total_assets_ttm_df.groupby('ticker').apply(lambda x: x['totalAssets' + '_TTM'].iloc[-1])

        return revenue_se / (total_assets_se + 0.0)


if __name__ == '__main__':
    from_dt = '2017-06-30'
    to_dt = '2018-04-09'

    # 取沪深300
    hs300 = ts.get_hs300s()
    hs300.code = hs300.code.apply(code_to_symbol)

    AssetTurnover = AssetTurnover(
        factor_name='AssetTurnover',
        factor_parameters={},
        tickers=hs300.code.tolist(),
        save_dir='',
        data_source='D:\\idwzx\\project\\factorset\\data',
    )

    AssetTurnover.generate_factor_and_store(from_dt, to_dt)
    print('因子构建完成，并已成功入库!')
