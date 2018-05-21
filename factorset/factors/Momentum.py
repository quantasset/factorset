# -*- coding:utf-8 -*-
"""
@author:code37
@file:Momentum.py
@time:2018/2/2317:37
"""

import tushare as ts
from factorset.factors import BaseFactor
from factorset.data.OtherData import code_to_symbol, shift_date
from factorset.data import CSVParser as cp

class Momentum(BaseFactor):
    """
    :名称: 动量因子，股票收益率
    :计算方法: 该指标的值等于最近三个月的股票收益率，利用当日和之前第252个交易日的复权价计算收益率，公式如下: Momentum_3M=(dajclose_price(t)/ dajclose_price(t-63)-1)
    """
    def __init__(self, factor_name='momentum_60D', tickers='000016.SH', factor_parameters={'lagTradeDays': 60}, data_source='', save_dir=None):
        # Initialize super class.
        super(Momentum, self).__init__(factor_name=factor_name, tickers=tickers,
                                   factor_parameters=factor_parameters,
                                   data_source=data_source, save_dir=save_dir)

        self.lagTradeDays = self.factor_param['lagTradeDays']

    def prepare_data(self, begin_date, end_date):
        """
        制作因子的数据准备
        :param begin_date: 
        :param end_date: 
        :return: 
        """
        shifted_begin_date = shift_date(begin_date, self.factor_param['lagTradeDays'])
        hq = cp.concat_stock(self.data_source, self.tickers).loc[shifted_begin_date:end_date, ['code', 'close']]
        self.hq = cp.hconcat_stock_series(hq, self.tickers)

    def generate_factor(self, end_day):
        """
        计算增量因子数据
        :param end_day: 因子生产的日期
        :return: pd.series，index为ticker，value为因子值
        """
        begin_day = shift_date(end_day, self.lagTradeDays)
        close_df = self.hq[begin_day: end_day]
        return close_df.iloc[-1] / close_df.iloc[0] - 1

if __name__ == '__main__':
    from_dt = '2017-06-30'
    to_dt = '2018-04-20'

    # 取沪深300
    hs300 = ts.get_hs300s()
    hs300.code = hs300.code.apply(code_to_symbol)

    # 实例化因子
    momentum_60D = Momentum(
        factor_name='momentum_60D',
        factor_parameters={'lagTradeDays': 60},
        tickers=hs300.code.tolist(),
        save_dir='',
        data_source='D:\\idwzx\\project\\factorset\\data',
    )

    # 生成因子数据并入库
    momentum_60D.generate_factor_and_store(from_dt, to_dt)
    print('因子构建完成，并已成功入库!')
