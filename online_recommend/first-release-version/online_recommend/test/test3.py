# i = 1
# print("=" * 20 + str(i) + "=" * 20)

# print("=======1=======")
# list_3 = lambda:[x for x in range(10)]
# print(list_3())
# print("=======2=======")
# i= 0
# list = [(lambda: i) for x in range(10)]
# for j in range(len(list)):
#     print(list[j](),end='')#
# print()
# print("========3======")
# list = [(lambda: x) for x in range(10)]
# for j in range(len(list)):
#     print(list[j](),end='')#
# print()
# print("========4======")
# list = [lambda: x for x in range(10)]
# for j in range(len(list)):
#     print(list[j](),end='')

from datetime import datetime, timedelta
import time
# t1 = 1384876800
# t2 = 1385049599
# t3 = int(time.time())
# print(t3)
# t = t3 - t3 % (3600 * 24)
# print(t)
# print(t3)
# t8 = datetime.now()
# print(t8)
# t = time.localtime(t)
# tt = time.localtime(t2)
# time_tmp = time.strftime('%Y-%m-%d %H:%M:%S', t)
# print(time_tmp)
# dt1 = datetime.utcfromtimestamp(t1)
# dt2 = datetime.utcfromtimestamp(t2)
# dt3 = datetime.utcfromtimestamp(t)
# print(type(dt3))
# print(dt3)
# print(dt2 - dt1)
# t00 = timedelta(1, 86399)
# t0 = timedelta(days=30)
# t000 = int(t0.total_seconds())
# print(t3 - t000)
# print(time)
# print((dt2 - dt1).seconds)
# print((dt2 - dt1).days)
# print((dt3-dt2).total_seconds())
# print((dt3-dt2).seconds/3600)
# print((dt3-dt2).days)
# import pytz
# tz = pytz.timezone('Asia/Shanghai')
# t4 = datetime.fromtimestamp(1537431607,tz)
# t5 = t4.strftime('%Y-%m-%d %H:%M:%S')
# t6 = t4.strftime('%Y-%m-%d')
#
# t7 = datetime.strptime(t5, '%Y-%m-%d %H:%M:%S')
# print(t4, t6, type(t5), type(t7))
# print((t7-t0).date())

from datetime import datetime, timedelta
# _yester = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
# print(_yester)
# start = datetime.strftime(_yester + timedelta(days=0, hours=-1, minutes=0), "%Y-%m-%d %H:%M:%S")
# end = datetime.strftime(_yester, "%Y-%m-%d")
# end = datetime.strftime(dt3, "%Y%m%d")
# print(start)
# print(end[-2:])
# print(type(datetime.now()))
# print(type(datetime.today().year))
# print(datetime.today().strftime('%Y-%m-%d'))

# a = '美国队长(普通话版)'
# # print(list(a))
# # print(a.split())
# import jieba
# print(jieba.lcut(a))
# print(len(a))

import numpy as np
# import math
# a = 1/(np.log2(10)+1)
# b = 1/(np.log2(500)+1)
# c = 1/(np.log2(1000)+1)
# d = 1/(np.log2(5000)+1)
# print(a/b, b/c, c/d)
# a = 1/(np.log10(10)+1)
# b = 1/(np.log10(500)+1)
# c = 1/(np.log10(1000)+1)
# d = 1/(np.log10(5000)+1)
# print(a/b, b/c, c/d)
# a = 1/(math.logs(10,100)+1)
# b = 1/(math.logs(500,100)+1)
# c = 1/(math.logs(1000,100)+1)
# d = 1/(math.logs(5000,100)+1)
# print(a/b, b/c, c/d)

# print(1 / np.exp(0.1), 1 / np.exp(2))
# print(1 / 2**0.1, 1 / 2**2)

# a = [1, 4, 7]
# b = [2, 5, 8]
# c = [3, 6, 9]
# print(list(zip(a, b, c)))

# print(np.array(c) + 2)


# from utils import day_timestamp
# def test(t):
#     print('hello world')
#     print(t)
#
# from apscheduler.schedulers.blocking import BlockingScheduler
# from apscheduler.executors.pool import ProcessPoolExecutor
# executors = {
#     'default': ProcessPoolExecutor(3)
# }
#
# scheduler = BlockingScheduler(executors=executors)
# # scheduler.add_job(test, "cron", day_of_week='*', hour='12', minute='45', second='10')
# # scheduler.add_job(test, "cron", day='*', hour='13', minute='9', second='10', args=[day_timestamp])
# scheduler.add_job(test, "cron", second='*', args=[day_timestamp])
# scheduler.start()

# import os
# print(os.listdir('data/data_count'))
# for a in  os.walk('data'):
#     print(a)

import time
import datetime
# # 今天日期
# today = datetime.date.today()
# # 昨天时间
# yesterday = today - datetime.timedelta(days=1)
#
# # from datetime import datetime
# # date_str  = datetime.strftime(today, "%Y%m%d")
# date_str  = today.strftime("%Y%m%d")
# yesterday_str = yesterday.strftime("%Y%m%d")
#
# timestamp = int(time.mktime(time.strptime(str(yesterday), '%Y-%m-%d')))  # 零点时间戳
# print(timestamp)
# print(type(today))
# print(yesterday)
# print(date_str)
# print(yesterday_str)

# a = 24
# b = 28
# c = a // 10 + 1
# d = b // 10
# e = int(a / 10)
# f = int(b / 10)
#
# print(c, d, e, f)
# from utils import day_timestamp
# from datetime import datetime
# print(day_timestamp)
# today_str = datetime.strftime(datetime.utcfromtimestamp(day_timestamp), "%Y%m%d %H%M%S")
# today_str2 = datetime.strftime(datetime.fromtimestamp(day_timestamp), "%Y%m%d %H%M%S")
# print(today_str, today_str2)

import datetime
#
# today = datetime.date.today()
# day_timestamp = int(time.mktime(time.strptime(str(today), '%Y-%m-%d')))  # 零点时间戳
# print(day_timestamp)

# today = datetime.date.today()
# yesterday = today - datetime.timedelta(days=1)
# day_timestamp = int(time.mktime(time.strptime(str(yesterday), '%Y-%m-%d')))  # 零点时间戳
# print(day_timestamp)

# from datetime import datetime
# timestamp = int(time.time())
# # date_time = int(datetime.strftime(datetime.utcfromtimestamp(timestamp), "%Y%m%d"))
# # 拿到本地的时间
# date_time = int(datetime.strftime(datetime.fromtimestamp(timestamp), "%Y%m%d"))
# print(date_time)

import time
import datetime

today = datetime.date.today()
day_timestamp = int(time.mktime(time.strptime(str(today), '%Y-%m-%d')))  # 零点时间戳
yesterday_time = day_timestamp - 3600 * 24

print(yesterday_time)