# -*- coding:utf-8 -*-
"""
@author:code37
@file:ROIC.py
@time:2018/3/217:39
"""

import pandas as pd
import tushare as ts
from factorset.factors import BaseFactor
from factorset.data.OtherData import code_to_symbol, shift_date
from factorset.data import CSVParser as cp
from factorset.Util.finance import ttmContinues

class ROIC(BaseFactor):
    def __init__(self, factor_name='ROIC', tickers='000016.SH', data_source='', factor_parameters={}, save_dir=None):
        # Initialize super class.
        super(ROIC, self).__init__(factor_name=factor_name, tickers=tickers,
                         factor_parameters=factor_parameters,
                         data_source=data_source, save_dir=save_dir)

    def prepare_data(self, begin_date, end_date):
        shifted_begin_date = shift_date(begin_date, 500)  # 取到2年之前的数据
        # Invested Capital = 资产总计121 - 流动负债101+ 应付票据68 + 短期借款109 + 一年内到期的长期负债0
        bs = cp.concat_fund(self.data_source, self.tickers, 'BS').loc[shifted_begin_date:end_date,['ticker', 121, 101, 68, 109, 0]]
        bs['IC'] = bs[121] - bs[101] + bs[68] + bs[109] + bs[0]
        bs = bs.drop([121, 101, 68, 109, 0], axis=1)
        self.bs = bs.dropna()

        # EBT = 归母净利润40 + 财务费用56
        inst = cp.concat_fund(self.data_source, self.tickers, 'IS').loc[shifted_begin_date:end_date,['ticker', 40, 56]]
        inst = inst[(inst[56] > 1) | (inst[56] < -1)].copy()
        inst['return'] = inst[40] + inst[56]
        inst = inst.drop([40, 56], axis=1)
        inst.dropna(inplace=True)

        inst['release_date'] = inst.index
        inst['report_date'] = inst.index

        returnTTM_ls = []
        for ticker in inst['ticker'].unique():
            try:  # 财务数据不足4条会有异常
                return_df = ttmContinues(inst[inst['ticker'] == ticker], 'return')
                return_df['ticker'] = ticker
            except:
                # print(ticker + ': revenue error')
                continue
            returnTTM_ls.append(return_df)

        self.inst = pd.concat(returnTTM_ls)
        self.inst.set_index('datetime', inplace=True)

    def generate_factor(self, end_day):
        inst = self.inst.loc[:end_day]
        inst.sort_index(ascending=True, inplace=True)
        inst = inst.groupby('ticker').apply(lambda x: x['return' + '_TTM'].iloc[-1])

        bs = self.bs.loc[:end_day]
        bs.sort_index(ascending=True, inplace=True)
        bs = bs.groupby('ticker')['IC'].rolling(5).mean().groupby('ticker').tail(1)

        ret = inst.dropna() / bs.reset_index(level=1, drop=True).dropna()
        return ret.dropna()


if __name__ == '__main__':
    from_dt = '2017-06-15'
    to_dt = '2018-03-09'

    # 取沪深300
    hs300 = ts.get_hs300s()
    hs300.code = hs300.code.apply(code_to_symbol)

    ROIC = ROIC(
        factor_name='ROIC',
        factor_parameters={},
        tickers=hs300.code.tolist(),
        save_dir='',
        data_source='D:\\idwzx\\project\\factorset\\data',
    )

    ROIC.generate_factor_and_store(from_dt, to_dt)
    print('因子构建完成，并已成功入库!')
