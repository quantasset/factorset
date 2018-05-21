# -*- coding:utf-8 -*-
"""
@author:code37
@file:UnleverBeta.py
@time:2018/3/123:35
"""
import pandas as pd
import numpy as np
import tushare as ts
from scipy import stats
from factorset.factors import BaseFactor
from factorset.data.OtherData import code_to_symbol, shift_date
from factorset.data import CSVParser as cp
from factorset.data import StockSaver as sp
from factorset.Util.finance import ttmContinues, ttmDiscrete


class UnleverBeta(BaseFactor):
    """
    :名称: UnleverBeta因子，剔除财务杠杆比率的Beta（账面价值比）
    :计算方法: UnleverBeta = Beta / (1 + 总负债 / 股东权益)
    :应用: Unlevered beta本意是作为一个实际beta估算的中间值，排除资本结构差异的影响。无财务杠杆的企业只有经营风险，没有财务风险，无财务杠杆的贝塔系数是企业经营风险的衡量，该贝塔系数越大，企业经营风险就越大，投资者要求的投资回报率就越大，市盈率就越低。
    
    """
    def __init__(self, factor_name='UnleverBeta_60D', tickers='000016.SH',
                 factor_parameters={'lagTradeDays': 60, 'benchmark': '000300'},
                 data_source='', save_dir=None):
        # Initialize super class.
        super(UnleverBeta, self).__init__(factor_name=factor_name, tickers=tickers,
                                   factor_parameters=factor_parameters,
                                   data_source=data_source, save_dir=save_dir)

        self.lagTradeDays = self.factor_param['lagTradeDays']
        self.benchmark = self.factor_param['benchmark']

    def prepare_data(self, begin_date, end_date):
        """
        数据预处理
        """
        # 多取一些数据做填充
        shifted_begin_date = shift_date(begin_date, self.factor_param['lagTradeDays'])

        # 获取股票行情
        hq = cp.concat_stock(self.data_source, self.tickers).loc[shifted_begin_date:end_date, ['code', 'close']]
        self.hq = cp.hconcat_stock_series(hq, self.tickers)

        # 获取指数Benchmark
        # b = sp.get_index(self.benchmark).loc[shifted_begin_date:end_date,['close']]
        b = pd.read_csv(self.data_source + '\\hq\\' + self.benchmark + '.csv', index_col=0).loc[
            shifted_begin_date:end_date, ['close']]
        self.b = b.fillna(method='ffill')

        # 获取财务数据
        # 按账面价值比 1/(1+负债总额/股东权益)
        # Dbequrt: Debt to Equity Ratio 产权比率=负债总额/股东权益*100%
        shifted_begin_date = shift_date(begin_date, 500)
        # 117负债, 121资产
        Dbequrt_df = cp.concat_fund(self.data_source, self.tickers, 'BS').loc[shifted_begin_date:end_date, ['ticker', 117, 121]]
        Dbequrt_df['totalLiabilities'] = Dbequrt_df[121]
        Dbequrt_df['totalEquity'] = Dbequrt_df[117]
        Dbequrt_df['Dbequrt'] = Dbequrt_df['totalLiabilities'] / Dbequrt_df['totalEquity']
        Dbequrt_df.drop([117, 121], axis=1, inplace=True)
        Dbequrt_df = Dbequrt_df[Dbequrt_df['Dbequrt'] :0]
        Dbequrt_df['report_date'] = Dbequrt_df.index
        Dbequrt_df['release_date'] = Dbequrt_df.index
        self.Dbequrt_df = Dbequrt_df.drop(['totalLiabilities', 'totalEquity'], axis=1)

        # 仅仅取年报, 查找是否reportDate是否以12月31日结尾
        # self.Dbequrt_df = Dbequrt_df[Dbequrt_df['reportDate'].str.endswith('1231')]

    def generate_factor(self, end_day):

        # 根据因子计算需要的数据量，计算出起始日期
        begin_day = shift_date(end_day, self.lagTradeDays)
        close_df = self.hq.loc[begin_day:end_day]
        close_b = self.b.loc[begin_day:end_day]

        ret = close_df / close_df.shift(1) - 1
        bret = close_b / close_b.shift(1) - 1
        bret.dropna(axis=0, how='any', inplace=True)

        # 根据因子滚动的数据时点，取最新一年的财报数据
        Dbequrt_df = self.Dbequrt_df[self.Dbequrt_df['release_date'] <= end_day]
        Dbequrt_df = Dbequrt_df.sort_values(by=['report_date', 'release_date'], ascending=[False, False])
        # 取最近1年的财报
        Dbequrt_df = Dbequrt_df.groupby('ticker').apply(lambda x: x.head(1))
        Dbequrt_df = Dbequrt_df['Dbequrt']

        # 查找无数据Ticker
        if not len(ret.dropna(axis=1, how='all').columns) == len(ret.columns):
            nonticker = list(set(ret.columns) - set(ret.dropna(axis=1, how='all').columns))
        else:
            nonticker = []

        beta = []
        for columns in ret:
            if columns in nonticker:
                beta.append(np.nan)
            else:
                # 每只股票数据量与Benchmark数据量对应
                retseries = ret[columns].dropna(axis=0, how='any')
                bseries = bret.iloc[len(bret) - len(retseries.index):len(bret)]
                OLSresult = stats.linregress(retseries.values, bseries.values.flatten())
                try:
                    beta.append(OLSresult[0])
                except IndexError:
                    beta.append(np.nan)

        beta_df = pd.DataFrame(beta, index=ret.columns, columns=['Beta'])
        beta_df = pd.concat((beta_df, Dbequrt_df.to_frame().reset_index(level=1, drop=True)), axis=1, join='inner')
        beta_df['UnleverBeta'] = beta_df['Beta'] / (1 + beta_df['Dbequrt'])

        return beta_df['UnleverBeta'].dropna()

if __name__ == '__main__':

    # 设定要需要生成的因子数据范围
    # 最多取到6-30
    from_dt = '2017-06-30'
    to_dt = '2018-04-20'

    # 取沪深300
    hs300 = ts.get_hs300s()
    hs300.code = hs300.code.apply(code_to_symbol)

    # 实例化因子
    UnleverBeta_60D = UnleverBeta(
        factor_name='UnleverBeta_60D',
        factor_parameters={'lagTradeDays': 60, 'benchmark': '000300'},
        tickers=hs300.code.tolist(),
        save_dir='',
        data_source='D:\\idwzx\\project\\factorset\\data',
    )

    # 生成因子数据并入库
    UnleverBeta_60D.generate_factor_and_store(from_dt, to_dt)
    print('因子构建完成，并已成功入库!')
