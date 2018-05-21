# -*- coding:utf-8 -*-
"""
@author:code37
@file:crawler_stable.py
@time:2018/4/1815:20
"""
import asyncio
import aiohttp
import async_timeout
import pandas as pd
import time
import os
from pandas.compat import StringIO
from tushare.stock import cons as ct
from factorset.Util.configutil import GetConfig

def balance_sheet_url(code):
    url = ct.SINA_BALANCESHEET_URL % code[0:6]
    return url


def profit_statement_url(code):
    url = ct.SINA_PROFITSTATEMENT_URL % code[0:6]
    return url


def cash_flow_url(code):
    url = ct.SINA_CASHFLOW_URL % code[0:6]
    return url


class FundCrawler(object):
    """
    **FundCrawler类，协程爬取基本面数据**
    
    """
    def __init__(self, TYPE):
        """
        :param TYPE: 'BS', 'IS', 'CF'
        """

        ############ SETTING #############
        self.config = GetConfig()
        self.TYPE = TYPE # 'BS', 'IS', 'CF'
        self.MONGO = self.config.MONGO
        self.CSV = self.config.CSV
        self.RAW = False
        self.outdir = self.config.fund_dir
        self.encode = self.config.encode
        self.proxypool = self.config.proxypool

        ############ CHANGE ABOVE SETTING #############

        if self.MONGO:
            from arctic import Arctic
            # mongod --dbpath D:/idwzx/project/arctic
            a = Arctic(self.config.ahost)
            a.initialize_library('ashare_{}'.format(self.TYPE))
            self.lib = a['ashare_{}'.format(self.TYPE)]

        self.result_dict = {}


    async def get_proxy(self):
        """
        获取proxy
        
        :return: proxy地址
        """
        async with aiohttp.ClientSession() as session:
            async with session.get("http://{}/get/".format(self.proxypool)) as response:
                proxy_str = await response.text()
                return "http://{}".format(proxy_str)

    async def fetch(self, queue, session, url, ticker):
        """
        单个ticker基本面爬取
        
        :param queue: ticker 队列
        :param session: aiohttp.ClientSession()
        :param url: 股票基本面爬取地址
        :param ticker: 股票代码
        :return: 基本面数据text
        """
        proxy_url = await self.get_proxy()
        print('proxy: ' + proxy_url)
        # ticker = url.split('/')[-3]
        try:
            async with async_timeout.timeout(15):
                async with session.get(url, proxy=proxy_url, allow_redirects=True) as resp:
                    if resp.status == 200 or 201:
                        return await resp.text()
                    else:
                        await queue.put(ticker)
        except Exception as e:
            print(e)
            print('Put {} in queue!'.format(ticker))
            await queue.put(ticker)

    async def consume(self, queue):
        """
        消费直到任务结束
        
        :param queue: ticker 队列
        :return: None
        """
        while True:
            ticker = await queue.get()
            async with aiohttp.ClientSession() as session:
                if self.TYPE == 'BS':
                    target_url = balance_sheet_url(ticker)
                elif self.TYPE == 'IS':
                    target_url = profit_statement_url(ticker)
                elif self.TYPE == 'CF':
                    target_url = cash_flow_url(ticker)
                else:
                    raise Exception
                html = await self.fetch(queue, session, target_url, ticker)
                if html:
                    self.result_dict[ticker] = html
                else:
                    queue.put(ticker)
            queue.task_done()

    async def run(self, queue, max_tasks):
        """
        Schedule the consumer
        
        :param queue: ticker 队列
        :param max_tasks: 最大协程数
        :return: None
        """
        tasks = [asyncio.ensure_future(self.consume(queue)) for _ in range(max_tasks)]
        await queue.join()
        for w in tasks:
            w.cancel()

    def write_to_MongoDB(self, symbol, df, source='Tushare'):
        """
        
        :param symbol: ticker
        :param df: 单个ticker基本面数据，pd.DataFrame
        :param source: 注释表明来源，str，默认为'Tushare'
        :return: None
        """
        try:
            self.lib.write(symbol, df, metadata={'source': source})
            print(symbol + '写入完成')
        except Exception as e:
            print("Failed for ", str(e))


    def data_clean(self, text):
        """
        text数据清洗
        
        :param text: 协程爬取的text数据 
        :return: pd.DataFrame
        """
        text = text.replace('\t\n', '\r\n')
        text = text.replace('\t', ',')
        df = pd.read_csv(StringIO(text), dtype={'code': 'object'})
        df = df.T.drop_duplicates()
        df.rename(columns=df.iloc[0], inplace=True)
        df.drop(df.index[0], inplace=True)
        df.index = pd.to_datetime(df.index)
        df.index.name = 'date'
        return df

    def main(self, Ashare, num=10, retry=2):
        """
        协程爬取主程序
        
        :param Ashare: 带爬取tickers
        :type Ashare: list 
        :param num: 最大协程数
        :type num: int
        :param retry: 重启次数
        :type retry: int
        :return: None
        """
        fail = []
        asyncio.set_event_loop(asyncio.new_event_loop())

        for i in range(retry):
            if fail:
                # Global event loop closed last time
                Ashare = list(set(fail))
                # Reset fail
                fail = []
                asyncio.set_event_loop(asyncio.new_event_loop())

            loop = asyncio.get_event_loop()
            queue = asyncio.Queue(loop=loop)

            for ticker in Ashare:
                queue.put_nowait(ticker)

            # Check if loop is closed, if it is, then break
            if loop.is_closed():
                return

            loop.run_until_complete(self.run(queue, num))
            loop.close()

            if not os.path.exists(self.outdir):
                os.mkdir(self.outdir)

            if self.RAW:
                try:
                    pd.DataFrame([self.result_dict]).to_csv(os.path.abspath("./fund/raw.csv"), encoding=self.encode)
                except Exception as e:
                    print(e)

            for key in self.result_dict:
                try:
                    df = self.data_clean(self.result_dict[key])
                    if len(df.values) == 0:
                        fail.append(key)
                    if self.MONGO:
                        self.write_to_MongoDB(key, df)
                    if self.CSV:
                        df.to_csv(os.path.abspath("{}/{}_{}.csv".format(self.outdir, self.TYPE, key)), encoding=self.encode)
                        print("{0}写入{1}条数据,".format(key, len(df)))
                except Exception as e:
                    print(e)
                    fail.append(key)
                    print("{}写入失败".format(key))
            if fail:
                print("{}表中{}爬取失败".format(self.TYPE, fail))
            else:
                print("{}表数据导入成功！".format(self.TYPE))

if __name__ == '__main__':

    start = time.time()
    import os
    from factorset.data.OtherData import code_to_symbol
    from factorset.data import CSVParser as cp
    import tushare as ts
    # allAshare = pd.read_csv(os.path.abspath('./allAShare.csv'))
    # allAshare = allAshare['0']
    hs300 = ts.get_hs300s()
    hs300.code = hs300.code.apply(code_to_symbol)
    # 爬取沪深300还未存入的数据
    Ashare = list(set(hs300.code.tolist()) - set(cp.all_fund_symbol(os.path.abspath('.'), 'IS')))
    # BS表内时间有重复
    # Ashare = ['300671.SZ', '002886.SZ', '300696.SZ', '603055.SH', '300670.SZ', '300692.SZ',
    # '002889.SZ', '603882.SH', '603801.SH', '603938.SH', '300687.SZ', '603535.SH', '603043.SH']
    # BS时间有重复且值不相同（招股说明与申报稿）
    # Ashare = ['002886.SZ', '300696.SZ', '603938.SH', '300692.SZ', '300670.SZ', '603882.SH']
    # IS时间有重复且值不相同（招股说明与申报稿）
    # Ashare = ['002886.SZ', '300696.SZ', '300670.SZ', '300692.SZ', '603055.SH', '603938.SH', '603882.SH']
    # CF时间有重复且值不相同（招股说明与申报稿）
    # Ashare = ['002386.SZ', '603882.SH', '603018.SH', '300671.SZ', '603938.SH', '300537.SZ', '300670.SZ' ,
    # '002086.SZ', '000568.SZ', '600612.SH', '300696.SZ', '600552.SH', '300687.SZ', '600983.SH', '002889.SZ',
    #  '603801.SH', '300692.SZ', '603055.SH', '002886.SZ', '002852.SZ', '603505.SH', '300365.SZ', '603535.SH',
    #  '300214.SZ', '300135.SZ', '603043.SH']
    FundCrawler('BS').main(Ashare, num=20)
    print(time.time()-start)

