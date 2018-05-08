# -*- coding:utf-8 -*-
"""
@author:code37
@file:test.py
@time:2018/5/721:38
"""
import asyncio
import random

result_dict = {}

async def fetch(ticker, queue):
    if random.randint(-1,2) < 0:
        print(str(ticker) + ":Fail!")
        await queue.put(ticker)
    else:
        asyncio.sleep(random.randint(1,2))
        print(str(ticker) + ":Done!")

async def consume(queue):
    while True:
        ticker = await queue.get()
        await fetch(ticker, queue)
        result_dict[ticker] = ticker
        queue.task_done()


async def run(queue, max_tasks):
    # schedule the consumer
    tasks = [asyncio.ensure_future(consume(queue)) for _ in range(max_tasks)]
    await queue.join()
    for w in tasks:
        w.cancel()

def main(l, num=10, retry=2):
    fail = []

    for i in range(retry):
        if fail:
            # Global event loop closed last time
            l = list(set(fail))
            # Reset fail
            fail = []
            asyncio.set_event_loop(asyncio.new_event_loop())
        # if fail = none, it can't get event loop and then exit
        try:
            loop = asyncio.get_event_loop()
        except Exception as e:
            print(e)
            return
        queue = asyncio.Queue(loop=loop)

        for ticker in l:
            queue.put_nowait(ticker)

        print(queue)
        loop.run_until_complete(run(queue, num))
        loop.close()

        for key in result_dict:
            if random.randint(-1,2) < 0:
                fail.append(key)

if __name__ == '__main__':
    l = range(20)
    main(l)