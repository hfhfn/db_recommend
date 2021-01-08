import time
import datetime

today = datetime.date.today()
yesterday = today - datetime.timedelta(days=1)
yesterday_str = yesterday.strftime("%Y%m%d")
yesterday_timestamp = int(time.mktime(time.strptime(str(yesterday), '%Y-%m-%d')))  # 零点时间戳
day = int(yesterday_str[-2:])
if day <= 10:
    date = yesterday_str[:-2] + '10'
elif day <= 20:
    date = yesterday_str[:-2] + '20'
else:
    date = yesterday_str[:-2] + '30'

import os
os.system("mysqldump --set-gtid-purged=OFF -h mysql  -uroot -pmysql -t \
            --where='ymd = {}' userdata \
                db_home_nav_content{}|gzip > /home/hadoop/user.sql.gz"
          .format(yesterday_timestamp, date))
