# -*- coding:utf-8 -*-
"""
@author:code37
@file:__init__.py
@time:2018/4/416:08
"""

import os
import tushare as ts
import pandas as pd
import numpy as np
from factorset.data import OtherData
from factorset.Util.configutil import GetConfig

if GetConfig().MONGO:
    from factorset.data import ArcticParser

class BaseFactor(object):
    """
    因子基类，用于因子的计算和存取
    当创建新的因子时，需要继承此类，并实现prepare_data,和generate_factor方法。
    """

    def __init__(self, factor_name, tickers, factor_parameters, data_source, save_dir=None, mongolib=None):
        """
        :param factor_name:  (str)因子名，必须唯一
        :param tickers: 计算因子的投资品种范围
        :param factor_parameters: (dict)因子计算使用到的自定义参数
        :param data_source: 数据源地址
        :param save_dir: 数据存储地址
        :param mongolib: MongoDB instance
        """

        self.__factor_name = factor_name
        self.factor_param = factor_parameters
        self.tickers = tickers
        # 储存计算的factor值
        self.__factor = []
        # 循环的trading_day
        self.__datetime = None
        # data路径
        if not save_dir:
            self.save_dir = os.path.abspath('./factor/{}.csv')
        else:
            self.save_dir = save_dir + '/factor/{}.csv'

        self.__mongolib = mongolib
        if data_source:
            self.data_source = data_source
        else:
            self.data_source = os.path.abspath('..')

        # 新股数据异常慢
        # self.ipo_info_df = ts.new_stocks()

    def get_factor_name(self):
        """
        获取因子唯一名称
        
        :return: (str)因子名
        """
        return self.__factor_name

    def prepare_data(self, from_date, to_date):
        """
        .. note::
           必须实现prepare_data方法，用于一次性获取需要的数据。
        
        :param from_date: 原始数据起始日
        :param to_date: 原始数据结束日
        """
        raise NotImplementedError

    def generate_factor(self, trading_day):
        """
        .. note::
           必须实现generate_factor方法，用于计算因子并返回因子的值。
        
        :param trading_day: 循环交易日
        """
        raise NotImplementedError

    def generate_factor_and_store(self, from_date, to_date):
        """
        计算因子并录入数据库

        :param from_date: (str)起始时间
        :param to_date: (str)结束时间
        :return: None
        """

        self.prepare_data(from_date, to_date)
        self.trading_days = self._get_trading_days(from_date, to_date)

        for trading_day in self.trading_days:
            df = self.generate_factor(trading_day).to_frame()
            df['ticker'] = df.index
            df['date'] = trading_day
            self.__factor.append(df)
        self.__factor = pd.concat(self.__factor)
        self.__factor.set_index('date', inplace=True)
        self.clear_factor()
        self.save()

    def clear_factor(self):
        """
         
        对当天的因子进行清洗，主要有:
        
        1. 过滤掉无穷大和无穷小的值
        
        2. 过滤掉nan值
        
        .. todo::
           Survivor bias check 
            
           3. 过滤掉未上市的股票（未上市可能已经有财报发布，导致会出现一些值）
           
           4. 过滤掉已经退市的股票
        
        :return: 过滤后的因子值
        """
        if self.__factor is None or len(self.__factor) == 0:
            return

        factor_se = self.__factor.replace([np.inf, -np.inf], np.nan)
        self.__factor = factor_se.dropna()

    def _get_trading_days(self, from_date, to_date):
        """
        获取计算因子的交易日历
        
        重写该函数可用于计算特定日期的因子， 如按月度进行计算
        
        :param from_date: (str)起始时间
        :param to_date: (str)结束时间
        :return: (list)交易日历
        """

        return OtherData.tradecal(from_date, to_date)

    def get_trading_days(self):
        """
        获取计算因子的交易日历
        
        :return: (list)交易日历
        """
        return self.trading_days

    def save(self):
        """
        存入数据
        """
        if self.__factor is None or len(self.__factor) == 0:
            return
        if self.__mongolib:
            ArcticParser.write_data(self.__mongolib, self.__factor_name, self.__factor)
        if self.save_dir:
            if not os.path.exists(self.save_dir.strip(self.save_dir.split('\\')[-1])):
                os.mkdir(self.save_dir.strip(self.save_dir.split('\\')[-1]))
            self.__factor.to_csv(self.save_dir.format(self.__factor_name))

