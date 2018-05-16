# -*- coding:utf-8 -*-
"""
@author:code37
@file:configutil.py
@time:2018/5/79:37
"""
import os
from configparser import ConfigParser

# 参考 proxy_pool.Util.GetConfig

class ConfigParse(ConfigParser):
    """
    rewrite ConfigParser, for support upper option
    """

    def __init__(self):
        ConfigParser.__init__(self)

    def optionxform(self, optionstr):
        return optionstr

class LazyProperty(object):
    """
    LazyProperty
    https://blog.csdn.net/handsomekang/article/details/39933553
    参考 proxy_pool.Util.GetConfig
    """

    def __init__(self, func):
        self.func = func

    def __get__(self, instance, owner):
        if instance is None:
            return self
        else:
            value = self.func(instance)
            setattr(instance, self.func.__name__, value)
            return value

class GetConfig(object):
    """
    To get config from config.ini
    参考 proxy_pool.Util.GetConfig
    """

    def __init__(self):
        self.pwd = os.path.split(os.path.realpath(__file__))[0]
        self.config_path = os.path.join(os.path.split(self.pwd)[0], 'CONFIG.ini')
        self.config_file = ConfigParse()
        self.config_file.read(self.config_path)

    @LazyProperty
    def target(self):
        return self.config_file.get('TARGET', 'all')

    @LazyProperty
    def hq_dir(self):
        return self.config_file.get('STORE', 'hqdir')

    @LazyProperty
    def fund_dir(self):
        return self.config_file.get('STORE', 'funddir')

    @LazyProperty
    def other_dir(self):
        return self.config_file.get('STORE', 'otherdir')

    @LazyProperty
    def ahost(self):
        return self.config_file.get('ARCTIC', 'host')

    @LazyProperty
    def MONGO(self):
        return self.config_file.getboolean('OPTIONS', 'MONGO')

    @LazyProperty
    def CSV(self):
        return self.config_file.getboolean('OPTIONS', 'CSV')

    @LazyProperty
    def SFL(self):
        return self.config_file.getboolean('OPTIONS', 'SFL')

    @LazyProperty
    def options(self):
        return self.config_file.options('OPTIONS')

    @LazyProperty
    def proxymin(self):
        return self.config_file.getint('READ','proxymin')

    @LazyProperty
    def encode(self):
        return self.config_file.get('READ','encode')

    @LazyProperty
    def proxypool(self):
        return self.config_file.get('READ', 'proxypool')

if __name__ == '__main__':
    gc = GetConfig()
    print(gc.target)
    print(gc.hq_dir)
    print(gc.fund_dir)
    print(gc.other_dir)
    print(gc.ahost)
    print(gc.options)
    print(gc.encode)
    print(gc.proxymin)
    print(gc.MONGO)
    print(gc.CSV)
    print(gc.SFL)
    print(gc.proxypool)
