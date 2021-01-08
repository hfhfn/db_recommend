from utils import UserDataApp, MovieDataApp, UpdateMovieApp, UpdateUserApp

# 影视频道信息
channelInfo = {
            "电影": 1969,      # 45349
            "电视剧": 1970,    # 12445
            "综艺": 1971,      # 76941
            "动漫": 1972,      # 14915
            "少儿": 1973,      # 29137
            "纪录片": 1974,    # 47983
            "体育": 2263,
            "MV": 2264,
            "健身": 2265,
            "教育": 2271,      # 800
            "知识": 2272,      # 35984
            "游戏": 2273,
            "音乐": 2302,
            "搞笑": 2304,
            "旅游": 2305,
            "汽车": 2306,
            "其他": 0,         # 2613
            "VIP": 3003,
            "短视频": 3005
        }

"""
# 推荐频道选择
"""


"""
# 增量基于电影召回参数
"""
um_spark = UpdateMovieApp().spark

# 时间模块，赋值给变量，在其他函数调用，只会保留第一次生成的时间，不会动态改变
# 必须把时间模块写入使用函数内部才能 每次运行都重新生成时间
"""
# import time
# import datetime
# timestamp = int(time.time())
# 当天零点时间戳(本地是北京时间，time.time获取时间戳时需要先+8时区，再取整到零点）
# day_timestamp = timestamp - (timestamp + 3600 * 8) % (3600 * 24)

# today = datetime.date.today()
# yesterday = today - datetime.timedelta(days=1)
# yesterday_str = yesterday.strftime("%Y, %m, %d, %H %M %S")
# timestamp = int(time.mktime(time.strptime(str(yesterday), '%Y-%m-%d')))  # 零点时间戳
"""

from datetime import timedelta
# 自定义时间间隔（天）
delta_day = 7
# 增量更新时间间隔时间戳
interval = int(timedelta(days=delta_day).total_seconds())
# start_time = day_timestamp - interval
# yesterday = day_timestamp - 3600 * 24


"""
增量基于用户召回参数
"""
uu_spark = UpdateUserApp().spark
# from datetime import datetime
# today_str = datetime.strftime(datetime.utcfromtimestamp(day_timestamp), "%Y%m%d %H:%M:%S")
# today_str = datetime.strftime(datetime.fromtimestamp(day_timestamp), "%Y%m%d")
# print(day_timestamp, today_str)

"""
数据库参数
"""
# 电影原始数据表
movie_original_tb = 'tp_page'
#上线数据库
online_db = 'dbys_online'
# 电影原始数据库
movie_original_db = 'dbys'
# 用户原始数据库
# user_original_db = 'dbys_user'
user_original_db = 'dbys'
# 影响因子数据库
factor_db = 'dbys_factor'
# 用户数据预处理数据库
user_pre_db = 'dbys_pre_user'
# 基于电影增量更新数据库
update_movie_db = 'dbys_update_movie'
# 基于用户增量更新数据库
update_user_db = 'dbys_update_user'
# 基于协同过滤数据库
als_db = 'dbys_als'


"""
地址参数
"""
# hadoop host
hadoop_host = 'hadoop-master:8020'
# 模型保存位置， model的保存地址需要提前创建好
movie_model_path = 'hdfs://{}/dbys_movie/models/'.format(hadoop_host)
# CountVectorizer模型保存位置
cv_path = "hdfs://hadoop-master:8020/dbys_movie/models/CV.model"
# tfidf模型保存位置
idf_path = "hdfs://hadoop-master:8020/dbys_movie/models/IDF.model"


"""
# 电影画像参数
"""
# 电影画像数据库
movie_portrait_db = 'dbys_movie_portrait'
# 电影画像词数量
mf_topK = 30


"""
# 用户画像参数
"""
# 用户画像数据库
user_portrait_db = 'dbys_user_portrait'
# 电影画像词数量
uf_topK = 30


"""
# 基于电影召回模块参数
"""
# 电影向量维度
dim = 100
movie_recall_db = 'dbys_movie_recall'
m_spark = MovieDataApp().spark
m_topK = 100
mCF_topK = 20
# 聚类中心个数
k = 50


"""
# 基于用户召回模块参数
"""
user_recall_db = 'dbys_user_recall'
u_spark = UserDataApp().spark
pre_topK = 1000
u_topK = 100






