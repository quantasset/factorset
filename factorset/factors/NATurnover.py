# -*- coding:utf-8 -*-
"""
@author:code37
@file:UnleverBeta.py
@time:2018/3/123:35
"""
import pandas as pd
import tushare as ts
from factorset.factors import BaseFactor
from factorset.data.OtherData import code_to_symbol, shift_date
from factorset.data import CSVParser as cp
from factorset.Util.finance import ttmContinues, ttmDiscrete

class NATurnover(BaseFactor):
    """
    净资产周转率

    NATurnover = revenue_TTM / netAsset_TTM
    """
    def __init__(self, factor_name='NATurnover', tickers='000016.SH', data_source='', factor_parameters={}, save_dir=None):
        # Initialize super class.
        super().__init__(factor_name=factor_name, tickers=tickers,
                         factor_parameters=factor_parameters,
                         data_source=data_source, save_dir=save_dir)


    def prepare_data(self, begin_date, end_date):
        """
        数据预处理
        """

        #  净资产周转率 = 营业收入_TTM / 净资产总计_TTM
        #  净资产总计=总资产-负债总额
        #  营业收入_TTM为最近4个季度报告期的营业收入之和，
        #  净资产总计_TTM为最近5个季度报告期总资产的平均值。
        #  Net asset turnover ratio = netAssets / totalLiabilities

        #  获取财务数据:
        shifted_begin_date = shift_date(begin_date, 500)
        #117负债, 121资产
        netAssets = cp.concat_fund(self.data_source, self.tickers, 'BS').loc[shifted_begin_date:end_date, ['ticker', 117, 121]]
        netAssets['netAssets'] =  netAssets[121] - netAssets[117]
        netAssets.drop([117, 121], axis=1, inplace=True)
        netAssets = netAssets[netAssets['netAssets'] > 0]
        netAssets['report_date'] = netAssets.index
        netAssets['release_date'] = netAssets.index

        netAssetsTTM_ls = []
        for ticker in netAssets['ticker'].unique():
            try:
                netAssets_df = ttmDiscrete(netAssets[netAssets['ticker'] == ticker], 'netAssets')
                netAssets_df['ticker'] = ticker
            except:
                # print(ticker + ': net asset error')
                continue
            netAssetsTTM_ls.append(netAssets_df)

        #0营业收入
        revenue = cp.concat_fund(self.data_source, self.tickers, 'IS').loc[shifted_begin_date:end_date, ['ticker', 0]]
        revenue['revenue'] =  revenue[0]
        revenue.drop([0], axis=1, inplace=True)
        revenue['report_date'] = revenue.index
        revenue['release_date'] = revenue.index

        revenueTTM_ls = []
        for ticker in revenue['ticker'].unique():
            try:  # 财务数据不足4条会有异常
                reven_df = ttmContinues(revenue[revenue['ticker'] == ticker], 'revenue')
                reven_df['ticker'] = ticker
            except:
                # print(ticker + ': revenue error')
                continue
            revenueTTM_ls.append(reven_df)

        self.revenueTTM = pd.concat(revenueTTM_ls)
        self.netAssetsTTM = pd.concat(netAssetsTTM_ls)

        # 仅仅取年报, 查找是否report_date是否以12月31日结尾
        # self.df = df[df['report_date'].str.endswith('1231')]

    def generate_factor(self, end_day):
        revenue_ttm_df = self.revenueTTM[self.revenueTTM['datetime'] <= end_day]
        net_assets_ttm_df = self.netAssetsTTM[self.netAssetsTTM['datetime'] <= end_day]

        # ttm 的值顺序排列，取最底部的数据即为最新的数据
        revenue_se = revenue_ttm_df.groupby('ticker').apply(lambda x: x['revenue' + '_TTM'].iloc[-1])
        net_assets_se = net_assets_ttm_df.groupby('ticker').apply(lambda x: x['netAssets' + '_TTM'].iloc[-1])

        return revenue_se / net_assets_se

if __name__ == '__main__':
    # 设定要需要生成的因子数据范围
    from_dt = '2017-06-15'
    to_dt = '2018-03-09'

    # 取沪深300
    hs300 = ts.get_hs300s()
    hs300.code = hs300.code.apply(code_to_symbol)

    # 实例化因子
    NATurnover = NATurnover(
        factor_name='NATurnover',
        factor_parameters={},
        tickers=hs300.code.tolist(),
        save_dir='',
        data_source='D:\\idwzx\\project\\factorset\\data',
    )

    # 生成因子数据并入库
    NATurnover.generate_factor_and_store(from_dt, to_dt)
    print('因子构建完成，并已成功入库!')

