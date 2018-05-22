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
To use factorset in a project, we need to inherit the :class:`~factorset.factors.BaseFactor` for the New Factor:

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
    :Name: NewFactor
    :Cal_Alg: NewFactor = blablabla
    :App: NewFactor can never blow up your account!

    """

    def __init__(self, factor_name='NewFactor', tickers='000016.SH', data_source='', factor_parameters={}, save_dir=None):
        # Initialize super class.
        super(NewFactor, self).__init__(factor_name=factor_name, tickers=tickers,
                                     factor_parameters=factor_parameters,
                                     data_source=data_source, save_dir=save_dir)

    def prepare_data(self, begin_date, end_date):

        self.data = cp.choose_a_dataset()

    def generate_factor(self, trading_day):

        factor_at_trading_day = awesome_func(self.data)

        return factor_at_trading_day

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

.. autoclass:: factorset.factors.NewFactor.NewFactor
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

