import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(BASE_DIR))
# sys.path.append(BASE_DIR)

import pytz
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
from apscheduler.executors.pool import ProcessPoolExecutor
from utils import logger as lg
from utils.default import channelInfo
from content_recall.update import Update, train_recall, train_movie_profile, train_user_profile


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

# tz = pytz.timezone('Asia/Shanghai')
# coalesce=True, 之前多次未执行成功的任务，不会重复执行，只会执行一次
# misfire_grace_time 任务超时容错
scheduler = BlockingScheduler(executors=executors, coalesce=True, max_instance=5, misfire_grace_time=3600)


"""
每周进行一次全量计算
"""
# scheduler.add_job(train_movie_profile, "cron", day_of_week='sat', hour='12', minute='0', second='0', args=[True])
# scheduler.add_job(train_user_profile, "cron", day_of_week='sun', hour='9', minute='0', second='0', args=[True])
#
# channel = '电影'
# cate_id = channelInfo[channel]
# scheduler.add_job(train_recall, "cron", day_of_week='sun', hour='10', minute='30', second='0',
#                   args=[channel, cate_id])
#
# channel = '电视剧'
# cate_id = channelInfo[channel]
# scheduler.add_job(train_recall, "cron", day_of_week='sun', hour='14', minute='30', second='0',
#                   args=[channel, cate_id])
#
# channel = '综艺'
# cate_id = channelInfo[channel]
# scheduler.add_job(train_recall, "cron", day_of_week='sun', hour='16', minute='30', second='0',
#                   args=[channel, cate_id])
#
# channel = '少儿'
# cate_id = channelInfo[channel]
# scheduler.add_job(train_recall, "cron", day_of_week='sun', hour='18', minute='30', second='0',
#                   args=[channel, cate_id])


"""
每天进行增量计算
"""
#movie_profile的开始时间
#start_hour
m_sh = 0
#start_minute
m_sm = 5
#user_profile的开始时间
#start_hour = 2
sh = 3
#start_minute = 30
sm = 30
# 总计算用时大约5个小时，切勿超过当天24点，否则下面时间公式不对

# 添加定时任务：用户电影推荐，每天2点30运行一次   mon,tue,wed,thu,fri,sat,sun
scheduler.add_job(train_movie_profile, "cron", day_of_week='*', hour='{}'.format(m_sh), minute='{}'.format(m_sm), \
                  second='0', args=[False])
scheduler.add_job(train_user_profile, "cron", day_of_week='*', hour='{}'.format(sh), minute='{}'.format(sm), \
                  second='0', args=[False])

# 相较前一个任务增加1个小时30分钟 即4点
movie_sh = sh+int((90+sm)/60)
movie_sm = (sm+30)%60
channel = '电影'
cate_id = channelInfo[channel]
scheduler.add_job(train_recall, "cron", day_of_week='*', hour='{}'.format(movie_sh),\
                  minute='{}'.format(movie_sm), second='0', args=[channel, cate_id])

# 相较前一个任务增加50分钟 即4点50
tv_sh = movie_sh + int((50+movie_sm)/60)
tv_sm = (movie_sm + 50)%60
channel = '电视剧'
cate_id = channelInfo[channel]
scheduler.add_job(train_recall, "cron", day_of_week='*', hour='{}'.format(tv_sh), \
                  minute='{}'.format(tv_sm), second='0', args=[channel, cate_id])

# 相较前一个任务增加50分钟 即5点40
zy_sh = tv_sh + int((50+tv_sm)/60)
zy_sm = (tv_sm + 50)%60
channel = '综艺'
cate_id = channelInfo[channel]
scheduler.add_job(train_recall, "cron", day_of_week='*', hour='{}'.format(zy_sh), \
                  minute='{}'.format(zy_sm), second='0', args=[channel, cate_id])

# 相较前一个任务增加1个小时20分钟 即7点
se_sh = zy_sh + int((80+tv_sm)/60)
se_sm = (zy_sm + 20)%60
channel = '少儿'
cate_id = channelInfo[channel]
scheduler.add_job(train_recall, "cron", day_of_week='*', hour='{}'.format(se_sh),\
                  minute='{}'.format(se_sm), second='0', args=[channel, cate_id])

"""
临时任务
"""
# scheduler.add_job(train_movie_profile, "cron", day_of_week='*', hour='14', minute='4', second='0', args=[True])
scheduler.add_job(train_user_profile, "cron", day_of_week='*', hour='15', minute='24', second='0', args=[True])

# channel = '电影'
# cate_id = channelInfo[channel]
# scheduler.add_job(train_recall, "cron", day_of_week='*', hour='9', minute='34', second='0',
#                   args=[channel, cate_id])
#
# channel = '电视剧'
# cate_id = channelInfo[channel]
# scheduler.add_job(train_recall, "cron", day_of_week='*', hour='10', minute='15', second='0',
#                   args=[channel, cate_id])
#
# channel = '综艺'
# cate_id = channelInfo[channel]
# scheduler.add_job(train_recall, "cron", day_of_week='*', hour='15', minute='5', second='0',
#                   args=[channel, cate_id])
#
# channel = '少儿'
# cate_id = channelInfo[channel]
# scheduler.add_job(train_recall, "cron", day_of_week='*', hour='17', minute='23', second='0',
#                   args=[channel, cate_id])


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

# import time
# import datetime
# today = datetime.date.today()
# _yesterday = today - datetime.timedelta(days=1)
# day_timestamp = int(time.mktime(time.strptime(str(today), '%Y-%m-%d')))  # 零点时间戳

# def test(t):
#     with open('/home/hadoop/test.txt', 'a', encoding='utf-8') as f:
#         f.write('hello world {}\n'.format(t))
# scheduler.add_job(test, trigger='interval', seconds=10, args=[day_timestamp])
# scheduler.add_job(test, "cron", day_of_week='*', hour='22', minute='46', second='0', args=[day_timestamp])


# 启动
scheduler.start()
