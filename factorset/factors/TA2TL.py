# -*- coding:utf-8 -*-
"""
@author:code37
@file:UnleverBeta.py
@time:2018/3/123:35
"""
import pandas as pd
import numpy as np
import tushare as ts
from factorset.factors import BaseFactor
from factorset.data.OtherData import code_to_symbol, shift_date
from factorset.data import CSVParser as cp


class TA2TL(BaseFactor):
    """
    :名称: 资产负债比（百度百科），资产负债率（Debt Assets ratio）的倒数
    :计算方法: 资产负债比 = 资产总额 / 负债总额
    :应用: 资产所占的比重越大，企业的经营风险就比较低，但相对来说，企业的资金利用就不是太有效率。
    """
    def __init__(self, factor_name='TA2TL', tickers='000016.SH', data_source='', factor_parameters={}, save_dir=None):
        # Initialize super class.
        super().__init__(factor_name=factor_name, tickers=tickers,
                         factor_parameters=factor_parameters,
                         data_source=data_source, save_dir=save_dir)


    def prepare_data(self, begin_date, end_date):
        """
        数据预处理
        """

        # 获取财务数据:
        # 资产负债比 = 总资产 /  公司债务总额
        # TA2TL = totalAssets / totalLiabilities
        # 117负债, 121资产
        shifted_begin_date = shift_date(begin_date, 500)
        ff = cp.concat_fund(self.data_source, self.tickers, 'BS').loc[shifted_begin_date:end_date,['ticker', 117, 121]]
        # 这里以report date假定为announce date
        ff['reportDate'] = ff.index

        # 取出负债，资产总额数据
        ff['TA2TL'] = ff[117] / ff[121]
        ff.drop([117, 121], axis=1, inplace = True)

        self.df = ff

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
        df = self.df.loc[:end_day]
        df.sort_index(ascending=False, inplace=True)
        # 取最近的财报
        df = df.groupby('ticker').apply(lambda x: x.head(1))
        return df['TA2TL'].reset_index(level=1, drop=True).dropna()

if __name__ == '__main__':
    # 设定要需要生成的因子数据范围
    from_dt = '2017-06-15'
    to_dt = '2018-03-09'

    # 取沪深300
    hs300 = ts.get_hs300s()
    hs300.code = hs300.code.apply(code_to_symbol)

    # 实例化因子
    TA2TL = TA2TL(
        factor_name='TA2TL',
        factor_parameters={},
        tickers=hs300.code.tolist(),
        save_dir='',
        data_source='D:\\idwzx\\project\\factorset\\data',
    )

    # 生成因子数据并入库
    TA2TL.generate_factor_and_store(from_dt, to_dt)
    print('因子构建完成，并已成功入库!')

