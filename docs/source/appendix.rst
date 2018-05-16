API Reference
-------------

Data Fetchers
~~~~~~~~~~~~~

Stock Data
``````````
.. autofunction:: factorset.Run.data_fetch.data_fetch

.. autofunction:: factorset.data.StockSaver.write_all_stock

.. autofunction:: factorset.data.StockSaver.save_index

Other Data
``````````
.. autofunction:: factorset.data.OtherData.market_value

.. autofunction:: factorset.data.OtherData.tradecal

Fundamental Data
````````````````
.. autoclass:: factorset.data.FundCrawler.FundCrawler
   :members:
   :undoc-members:

Data Reader
~~~~~~~~~~~
The following methods are available for use in the ``prepare_data`` (recommended), ``generate_factor`` API functions.

Stock Data
``````````
.. autofunction:: factorset.data.CSVParser.all_stock_symbol

.. autofunction:: factorset.data.CSVParser.read_stock

.. autofunction:: factorset.data.CSVParser.concat_stock

.. autofunction:: factorset.data.CSVParser.concat_all_stock

.. autofunction:: factorset.data.CSVParser.hconcat_stock_series


Other Data
``````````
.. autofunction:: factorset.data.OtherData.write_new_stocks

.. autofunction:: factorset.data.OtherData.write_all_date

Fundamental Data
````````````````
.. autofunction:: factorset.data.CSVParser.all_fund_symbol

.. autofunction:: factorset.data.CSVParser.read_fund

.. autofunction:: factorset.data.CSVParser.fund_collist

.. autofunction:: factorset.data.CSVParser.concat_fund

Data Util
~~~~~~~~~
.. autofunction:: factorset.data.OtherData.code_to_symbol

.. autofunction:: factorset.data.OtherData.shift_date

.. autofunction:: factorset.Util.finance.ttmContinues

.. autofunction:: factorset.Util.finance.ttmDiscrete


Factor Object
~~~~~~~~~~~~~
In all listed functions, the ``self`` argument is implicitly the
currently-executing :class:`~factorset.factors.BaseFactor` instance.

.. autoclass:: factorset.factors.BaseFactor
   :members:
   :undoc-members:


