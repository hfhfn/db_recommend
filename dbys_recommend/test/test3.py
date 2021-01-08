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

# print("time.time(): %f " %  time.time())
# print(time.localtime( time.time() ))
# print(time.asctime( time.localtime(time.time()) ))
# print(int(time.time()) - (int(time.time()) % (3600 * 24)) - 3600 * 8)

# a = 1590087600 - (1590087600 + 3600 * 8) % (3600 * 24)
# print(a)

# li = [0]
# a = li * 10
# print(a)

# a = [i for i in range(10000000)]
# b = [i for i in range(9999999)]
# import time
# start = time.time()
# print([ i for i in a if i not in b ])  # 时间太长
# # 下面两个缺点是会改变原列表顺序
# print(list(set(a) ^ set(b)))   # 0.95
# print(list(set(a).difference(set(b))))   # 0.688
# end = time.time()
# print(end - start)

# a = [(1, 1), (2, 2), (3, 3)]
# print(dict(a))

# _negative = [1, 2, 3, 4]
# sample_count = 4
# print(dict(zip(_negative, [0] * sample_count)))

# import os
# os.makedirs('/home/movie_recommend/test/mkdir', mode=0o775)

# from collections import OrderedDict
# A = ['2016-01-01','2016-01-02','2016-01-03','2017-06-01','2016-03-08']
# B = ['2016-03-02','2016-03-08']
# d_set = OrderedDict.fromkeys(A)
# print(list(d_set.keys()))

from collections import defaultdict
# N = defaultdict(lambda : 0)  # 物品计数 （即此电影被多少用户看过）
# C = defaultdict(lambda :defaultdict(lambda : 0))  # 物品-物品-共现矩阵
#
# for i in range(10):
#     N[i] += 1
#     for j in [1, 2, 2, 3, 4, 4, 1, 2]:
#         C[i][j] += 1
#
# print(N[4], '----', C[1][4])
# print(N, '----', C[1])

# W = defaultdict(dict)  # 物品-物品-共现矩阵
# W[1][4] = 8
# print(W[1][4], W[1])

# x = {0: 'wpy', 1: 'scg', 2:{2:1}}
# np.save('../data/rating_matrix/test.npy',x)
# x = np.load('../data/rating_matrix/test.npy', allow_pickle=True).item()
# print(x[2][2])

# x = {0: 1, 1: 2, 2: 3}
# print(x.values())
# print(np.mean(list(x.values())))

# import pandas as pd
# a = ['jdfl', '6sdjfk', 'j4id']
# b = [{1:1}, 2, 3]
# df = pd.DataFrame()
# df['score'] = a
# df['num'] = b
# df['tag'] = range(3)
# print(df)
# df['test'] = pd.qcut(df['tag'], 2, labels=[1, 2])
# print(df)


# from multiprocessing import Process, Pipe
# import time
#
# def f(conn, tmp):
#     # time.sleep(3)
#     conn.send(tmp)
#     conn.close()
#
# parent_conn, child_conn = Pipe()
# ps = []
# for i in range(5):
#     tmp = 'haha' + str(i)
#     p = Process(target=f, args=(child_conn, tmp))
#     p.start()
#     ps.append(p)
#
# msg = []
# while True:
#    print('Test')
#    msg.append(parent_conn.recv())
#    if len(msg) == 5:
#       break
# # for p in ps:
# #     p.join()
# print(msg)
# print('The End')


# from operator import itemgetter
# relation = {1:1, 2:2, 3:1, 4:3, 5:4, 6:2}
# ret = sorted(relation.items(), key=itemgetter(1), reverse=True)
# print(ret)

# def sum_dict(a,b):
#     temp = dict()
#     for key in a.keys() | b.keys(): # 并集
#         temp[key] = sum([d.get(key, 0) for d in (a, b)])
#     return temp
#
# def sum_double_dict(a,b):
#     temp = defaultdict(dict)
#     for key in a.keys() | b.keys(): # 并集
#         for i in a[key].keys() | b[key].keys():
#             temp[key][i] = sum([d[key].get(i, 0) for d in (a, b)])
#     return temp
#
# def test():
#     #python3使用reduce需要先导入
#     from functools import reduce
#     #[a,b,c]列表中的参数可以2个也可以多个，自己尝试。
#     return print(reduce(sum_dict,[a,b,c]))
#
# a = {'a': 1, 'b': 2, 'c': 3}
# b = {'a':1,'b':3,'d':4}
# c = {'g':3,'f':5,'a':10}
# test()

# def main(*args):
#     print(args)
#
# def test(a, b, c):
#     print(a, b, c)
#
# main([1, 2, 3])
# main(1, 2, 3)
#
# param = [1, 2, 3]
# test(*param)
