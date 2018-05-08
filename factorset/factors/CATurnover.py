# -*- coding:utf-8 -*-
"""
@author:code37
@file:UnleverBeta.py
@time:2018/3/123:35
"""
import pandas as pd
import tushare as ts
from factorset.factors import BaseFactor
from factorset.data.OtherData import code_to_symbol, shift_date, market_value
from factorset.data import CSVParser as cp
from factorset.Util.finance import ttmContinues,ttmDiscrete

class CATurnover(BaseFactor):
    """
    流动资产周转率
    CATurnover = Revenue / Current Asset
    流动资产周转率 = 营业收入_TTM / 流动资产总计_TTM
    """

    def __init__(self, factor_name='CATurnover', tickers='000016.SH', data_source='', factor_parameters={}, save_dir=None):
        # Initialize super class.
        super(CATurnover, self).__init__(factor_name=factor_name, tickers=tickers,
                                     factor_parameters=factor_parameters,
                                     data_source=data_source, save_dir=save_dir)

    def prepare_data(self, begin_date, end_date):
        """
        数据预处理
        """
        # 获取财务数据:
        # CATurnover = currentAssets 103 / revenue 0
        shifted_begin_date = shift_date(begin_date, 500)
        bs = cp.concat_fund(self.data_source, self.tickers, 'BS').loc[shifted_begin_date:end_date, ['ticker', 103]]
        bs['release_date'] = bs.index
        bs['report_date'] = bs.index
        bs['currentAssets'] = bs[103]
        bs.drop(103, axis=1, inplace=True)

        inst = cp.concat_fund(self.data_source, self.tickers, 'IS').loc[shifted_begin_date:end_date, ['ticker', 0]]
        inst['release_date'] = inst.index
        inst['report_date'] = inst.index
        inst['revenue'] = inst[0]
        inst.drop([0], axis=1, inplace=True)

        # TTM Continues处理
        revenueTTM_ls = []
        for ticker in inst['ticker'].unique():
            try:  # 财务数据不足4条会有异常
                reven_df = ttmContinues(inst[inst['ticker'] == ticker], 'revenue')
                reven_df['ticker'] = ticker
            except:
                print(ticker + ': revenue error')
                continue
            revenueTTM_ls.append(reven_df)

        # TTM Discrete 取近期平均
        currentAssetsTTM_ls = []
        for ticker in bs['ticker'].unique():
            try:
                currentAssets_df = ttmDiscrete(bs[bs['ticker'] == ticker], 'currentAssets')
                currentAssets_df['ticker'] = ticker
            except:
                print(ticker + ': current asset error')
                continue
            currentAssetsTTM_ls.append(currentAssets_df)

        self.revenueTTM = pd.concat(revenueTTM_ls)
        self.currentAssetsTTM = pd.concat(currentAssetsTTM_ls)

        # 仅仅取年报, 查找是否reportDate是否以12月31日结尾
        # self.df = df[df['reportDate'].str.endswith('1231')]

    def generate_factor(self, end_day):
        """
        逐日生成因子数据

        Parameters
        -----------
        end_day:
            因子生产的日期

        Returns
        -----------
        ret: pd.Series类型 
             indx为ticker，value为因子值
        """
        revenue_ttm_df = self.revenueTTM[self.revenueTTM['datetime'] <= end_day]
        current_assets_ttm_df = self.currentAssetsTTM[self.currentAssetsTTM['datetime'] <= end_day]

        # ttm 的值顺序排列，取最底部的数据即为最新的数据
        revenue_se = revenue_ttm_df.groupby('ticker').apply(lambda x: x['revenue' + '_TTM'].iloc[-1])
        current_assets_se = current_assets_ttm_df.groupby('ticker').apply(lambda x: x['currentAssets' + '_TTM'].iloc[-1])

        return revenue_se / current_assets_se

if __name__ == '__main__':
    # 设定要需要生成的因子数据范围
    from_dt = '2017-06-01'
    to_dt = '2018-04-09'

    # 取沪深300
    hs300 = ts.get_hs300s()
    hs300.code = hs300.code.apply(code_to_symbol)

    CATurnover = CATurnover(
        factor_name='CATurnover',
        factor_parameters={},
        tickers=hs300.code.tolist(),
        save_dir='',
        data_source='D:\\idwzx\\project\\factorset\\data',
    )

    CATurnover.generate_factor_and_store(from_dt, to_dt)
    print('因子构建完成，并已成功入库!')


