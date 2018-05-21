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

class LDebt2TA(BaseFactor):
    """
    :名称: 长期负债比率（Long-term liability rate）
    :计算方法: 长期负债比 = 长期负债 / 资产总额
    :应用: 长期负债比率越小，表明公司负债的资本化程度低，长期偿债压力小；反之，则表明公司负债的资本化程度高，长期偿债压力大。

    """

    def __init__(self, factor_name='LDebt2TA', tickers='000016.SH', data_source='', factor_parameters={}, save_dir=None):
        # Initialize super class.
        super().__init__(factor_name=factor_name, tickers=tickers,
                         factor_parameters=factor_parameters,
                         data_source=data_source, save_dir=save_dir)


    def prepare_data(self, begin_date, end_date):
        """
        数据预处理
        """

        # 获取财务数据:
        # 长期负债比 = 长期负债136 /  资产总额121
        # LDebt2TA = totalLongLiabilities / totalAssets
        shifted_begin_date = shift_date(begin_date, 500)
        df = cp.concat_fund(self.data_source, self.tickers, 'BS').loc[shifted_begin_date:end_date, ['ticker', 136, 121]]
        # 取出负债，资产总额数据
        df['LDebt2TA'] = df[136] / df[121]
        df['reportDate'] = df.index
        df['reportDate'] = df['reportDate'].apply(lambda x: x.strftime("%Y-%m-%d"))
        # 仅仅取年报, 查找是否reportDate是否以12月31日结尾
        df = df[df['reportDate'].str.endswith('12-31')]
        df.drop([136, 121], axis=1)

        self.df = df

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
        df = df.sort_values('reportDate', ascending=False)
        # 取最近1年的财报
        df = df.groupby('ticker').apply(lambda x: x.head(1))

        return df['LDebt2TA'].reset_index(level=1, drop=True).dropna()

if __name__ == '__main__':
    # 设定要需要生成的因子数据范围
    from_dt = '2017-06-15'
    to_dt = '2018-03-09'

    # 取沪深300
    hs300 = ts.get_hs300s()
    hs300.code = hs300.code.apply(code_to_symbol)

    # 实例化因子
    LDebt2TA = LDebt2TA(
        factor_name='LDebt2TA',
        factor_parameters={},
        tickers=hs300.code.tolist(),
        save_dir='',
        data_source='D:\\idwzx\\project\\factorset\\data',
    )

    # 生成因子数据并入库
    LDebt2TA.generate_factor_and_store(from_dt, to_dt)
    print('因子构建完成，并已成功入库!')


