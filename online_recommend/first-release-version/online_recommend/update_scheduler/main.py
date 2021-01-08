import os
import sys

from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)


import pytz
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.executors.pool import ProcessPoolExecutor
from update_scheduler import Update, train
from utils import logger as lg


# 初始化自定义日志打印的位置和名称
log = lg.create_logger()

# 任务监听
def my_listener(event):
    if event.exception:
        print('任务出错了！！！！！！')


# 创建scheduler，多进程执行
executors = {
    'default': ProcessPoolExecutor(6)
}

tz = pytz.timezone('Asia/Shanghai')
scheduler = BlockingScheduler(executors=executors, coalesce=True, timezone=tz)
#scheduler = BlockingScheduler(executors=executors, coalesce=True)


# 添加定时任务：用户电影推荐，每天2点30运行一次
scheduler.add_job(train, "cron", day_of_week='mon-sat', hour='2', minute='30', second='0', args=[False])
scheduler.add_job(train, "cron", day_of_week='sun', hour='2', minute='30', second='0', args=[True])
# scheduler.add_job(train, "cron", day_of_week='*', hour='19', minute='46', second='0')

scheduler.add_listener(my_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
# scheduler._logger = log


# 添加一个定时的电影画像以及召回更新任务，每个一个小时运行一次更新
# 3种模式：trigger，date，cron
# scheduler.add_job(update_movie_recall_run(), trigger='interval', hours=1)
# scheduler.add_job(update_movie_recall_run(), 'date', run_date=datetime(2019, 8, 30, 1, 0, 0))
# scheduler.add_job(update_movie_recall_run, "cron", day_of_week='*', hour='1', minute='30', second='30')

# # 初始化更新类
# up = Update()
# # 添加一个定时的电影画像以及召回更新任务，每天2点30运行一次
# scheduler.add_job(up.update_movie_recall_run, "cron", day_of_week='*', hour='2', minute='30', second='0')
# # 添加一个定时的用户行为历史的更新任务，每天3点20运行
# scheduler.add_job(up.update_user_history, "cron", day_of_week='*', hour='3', minute='20', second='0')
# # 添加一个定时的用户画像的更新任务，每天3点40运行
# scheduler.add_job(up.update_user_profile_run, "cron", day_of_week='*', hour='4', minute='0', second='0')
# # 添加一个定时的用户画像召回结果的任务
# # scheduler.add_job(update_profile_recall_run, "cron", day_of_week='*', hour='1', minute='0', second='0')
# # 添加一个定时的用户观看相似召回的更新任务，每天4点30运行
# scheduler.add_job(up.update_similar_recall_run, "cron", day_of_week='*', hour='4', minute='30', second='0')


# # 以下测试用

#from utils import day_timestamp
#def test(t):
#    import time
#    t2 = int(time.time())
#    with open('/home/hadoop/test.txt', 'a', encoding='utf-8') as f:
#        f.write('hello world {}, {}\n'.format(t, t2))
#scheduler.add_job(test, trigger='interval', seconds=10, args=[day_timestamp])
#scheduler.add_job(test, "cron", day_of_week='*', hour='12', minute='0', second='0', args=[day_timestamp])



# 启动
scheduler.start()
