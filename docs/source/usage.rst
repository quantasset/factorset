===============
Factorset Usage
===============

Module contents
~~~~~~~~~~~~~~~
In all listed functions, the ``self`` argument is implicitly the
currently-executing :class:`~factorset.factors.BaseFactor` instance.

.. autoclass:: factorset.factors.BaseFactor
    :members:
    :undoc-members:

Generate Factors
~~~~~~~~~~~~~~~~
To use factorset in a project, we may inherit the :class:`~factorset.factors.BaseFactor` for the New Factor:

.. code-block:: python

    import os
    import pandas as pd
    import tushare as ts
    from factorset.factors import BaseFactor
    from factorset.data.OtherData import code_to_symbol, shift_date, market_value
    from factorset.data import CSVParser as cp
    from factorset.Util.finance import ttmContinues

    class NewFactor(BaseFactor):
        """
        NewFactor = Calculate Method
        """
        def __init__(self, factor_name='NewFactor', tickers='000016.SH', data_source='', factor_parameters={}, save_dir=None):
            # Initialize super class.
            super(NewFactor, self).__init__(factor_name=factor_name, tickers=tickers,
                                         factor_parameters=factor_parameters,
                                         data_source=data_source, save_dir=save_dir)

        def prepare_data(self, begin_date, end_date):
            shifted_begin_date = shift_date(begin_date, 500)
            # motherNetProfit 40
            inst = cp.concat_fund(self.data_source, self.tickers, 'IS').loc[shifted_begin_date:end_date,['ticker', 40]]
            inst['release_date'] = inst.index
            inst['report_date'] = inst.index
            # cash_flows_yield 133
            cf = cp.concat_fund(self.data_source, self.tickers, 'CF').loc[shifted_begin_date:end_date,['ticker', 133]]
            cf['release_date'] = cf.index
            cf['report_date'] = cf.index

            self.accrual_df = cf.merge(inst, on=['ticker', 'release_date', 'report_date'])
            self.accrual_df['accr'] = self.accrual_df[40] - self.accrual_df[133]

            cash_flow_ls = []
            for ticker in self.accrual_df['ticker'].unique():
                try:
                    reven_df = ttmContinues(self.accrual_df[self.accrual_df['ticker'] == ticker], 'accr')
                    reven_df['ticker'] = ticker
                except:
                    continue
                cash_flow_ls.append(reven_df)

            self.accrual_ttm = pd.concat(cash_flow_ls)
            # 总市值
            # Tushare的市值数据只有17年-now
            df = market_value(self.data_source + '\\other\\otherdata.csv', self.tickers)
            self.mkt_value = df.drop(['price', 'totals'], axis=1)

        def generate_factor(self, trading_day):
            accr_df = self.accrual_ttm[self.accrual_ttm['datetime'] <= trading_day]
            accr_df = accr_df.sort_values(by=['datetime', 'report_date'], ascending=[False, False])
            accr_se = accr_df.groupby('ticker')['accr_TTM'].apply(lambda x: x.iloc[0])  # 取最近1年的财报

            today_mkt_value = self.mkt_value.loc[trading_day]
            mkt_value = today_mkt_value.set_index('ticker')['mkt_value']
            ret_se = accr_se / mkt_value
            return ret_se.dropna()


    if __name__ == '__main__':
        from_dt = '2017-07-15'
        to_dt = '2018-04-09'

        # 取沪深300
        hs300 = ts.get_hs300s()
        hs300.code = hs300.code.apply(code_to_symbol)

        NewFactor = NewFactor(
            factor_name='NewFactor',
            factor_parameters={},
            tickers=hs300.code.tolist(),
            save_dir='',
            data_source=os.path.abspath('.'),
        )

        NewFactor.generate_factor_and_store(from_dt, to_dt)
        print('因子构建完成，并已成功入库!')


.. note:: The ``data_source`` can be designated when using Arctic or MongoDB.

Factors Set
~~~~~~~~~~~

Accruals2price
--------------

.. autoclass:: factorset.factors.Accruals2price.Accruals2price
    :members:
    :undoc-members:
    :show-inheritance:

AssetTurnover
-------------

.. autoclass:: factorset.factors.AssetTurnover.AssetTurnover
    :members:
    :undoc-members:
    :show-inheritance:

Beta
----

.. autoclass:: factorset.factors.Beta.Beta
    :members:
    :undoc-members:
    :show-inheritance:

CATurnover
----------

.. autoclass:: factorset.factors.CATurnover.CATurnover
    :members:
    :undoc-members:
    :show-inheritance:

CurrentRatio
------------

.. autoclass:: factorset.factors.CurrentRatio.CurrentRatio
    :members:
    :undoc-members:
    :show-inheritance:

EP\_LYR
-------

.. autoclass:: factorset.factors.EP_LYR.EP_LYR
    :members:
    :undoc-members:
    :show-inheritance:

EP\_TTM
-------

.. autoclass:: factorset.factors.EP_TTM.EP_TTM
    :members:
    :undoc-members:
    :show-inheritance:

GPOA
----

.. autoclass:: factorset.factors.GPOA.GPOA
    :members:
    :undoc-members:
    :show-inheritance:

GrossMarginTTM
--------------

.. autoclass:: factorset.factors.GrossMarginTTM.GrossMarginTTM
    :members:
    :undoc-members:
    :show-inheritance:

InterestCover
-------------

.. autoclass:: factorset.factors.InterestCover.InterestCover
    :members:
    :undoc-members:
    :show-inheritance:

LDebt2TA
--------

.. autoclass:: factorset.factors.LDebt2TA.LDebt2TA
    :members:
    :undoc-members:
    :show-inheritance:

Momentum
--------

.. autoclass:: factorset.factors.Momentum.Momentum
    :members:
    :undoc-members:
    :show-inheritance:

NATurnover
----------

.. autoclass:: factorset.factors.NATurnover.NATurnover
    :members:
    :undoc-members:
    :show-inheritance:

QuickRatio
----------

.. autoclass:: factorset.factors.QuickRatio.QuickRatio
    :members:
    :undoc-members:
    :show-inheritance:

ROIC
----

.. autoclass:: factorset.factors.ROIC.ROIC
    :members:
    :undoc-members:
    :show-inheritance:

RoeGrowth1
----------

.. autoclass:: factorset.factors.RoeGrowth1.RoeGrowth1
    :members:
    :undoc-members:
    :show-inheritance:

RoeGrowth2
----------

.. autoclass:: factorset.factors.RoeGrowth2.RoeGrowth2
    :members:
    :undoc-members:
    :show-inheritance:

TA2TL
-----

.. autoclass:: factorset.factors.TA2TL.TA2TL
    :members:
    :undoc-members:
    :show-inheritance:

UnleverBeta
-----------

.. autoclass:: factorset.factors.UnleverBeta.UnleverBeta
    :members:
    :undoc-members:
    :show-inheritance:

