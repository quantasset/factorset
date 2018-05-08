# -*- coding:utf-8 -*-
"""
@author:code37
@file:Proxy_start.py
@time:2018/4/199:11
"""

import sys
import proxy_pool

sys.path.append(proxy_pool.__file__.strip(proxy_pool.__file__.split('\\')[-1]))

import requests
from proxy_pool.Run.main import run
from factorset.Util.configutil import GetConfig


gc = GetConfig()

def get_proxy():
    return requests.get("http://{}/get/".format(gc.proxypool)).content

def get_all_proxy():
    return requests.get("http://{}/get_all/".format(gc.proxypool)).json()

def r():
    run()
# sys.path.append('D:/Anaconda3/envs/idwzx/Lib/site-packages/proxy_pool/')

if __name__ == '__main__':
    r()


