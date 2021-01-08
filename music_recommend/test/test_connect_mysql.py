import pymysql
import datetime


today = datetime.date.today()
date_str  = today.strftime("%Y%m%d")

## 登录账号
host = "rm-wz9pfz286p35r1iqqwo.mysql.rds.aliyuncs.com"
user = "aidb"
password = "Tuantuan2012@"
database = "aidb"
port = 3306

conn = pymysql.connect(host, user, password, database, port) ## 登录数据库，返回连接

cursor = conn.cursor()

insert_movie_sql = "INSERT INTO yh_movie_tv_zy_recall(user_id, movie_id, title, sort_num, `timestamp`, cate_id) \
            SELECT user_id, movie_id, title, sort_num, `timestamp`, 1969 cate_id FROM yh_movie_recall \
            WHERE sort_num <= 20 AND `timestamp` = {};".format(date_str)

insert_tv_sql = "INSERT INTO yh_movie_tv_zy_recall(user_id, movie_id, title, sort_num, `timestamp`, cate_id) \
            SELECT user_id, movie_id, title, sort_num, `timestamp`, 1970 cate_id FROM yh_tv_recall \
            WHERE sort_num <= 20 AND `timestamp` = {};".format(date_str)

insert_zy_sql = "INSERT INTO yh_movie_tv_zy_recall(user_id, movie_id, title, sort_num, `timestamp`, cate_id) \
            SELECT user_id, movie_id, title, sort_num, `timestamp`, 1971 cate_id FROM yh_zy_recall \
            WHERE sort_num <= 20 AND `timestamp` = {};".format(date_str)

try:
   # Execute the SQL command
   cursor.execute(insert_movie_sql)
   cursor.execute(insert_tv_sql)
   cursor.execute(insert_zy_sql)
   # Commit your changes in the database
   conn.commit()
except:
   # Rollback in case there is any error
   conn.rollback()

# data = cursor.fetchall() ##data为以元组为元素的元组
cursor.close()
conn.close() ## 关闭连接
