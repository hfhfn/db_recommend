# spark = MovieDataApp().spark
# spark.sql("create database if not exists test comment 'test' location '/user/hive/warehouse/test.db'")
# spark.sql("select * from portrait.movie_feature").show(10, truncate=False)
# spark.sql("select * from portrait.textrank").show(10, truncate=False)

# count = spark.sql("select * from portrait.movie_feature").count()
# count2 = spark.sql("select * from portrait.movie_feature").dropna().count()
# print(count)
# print(count2)

# f = open('./data/stopwords/all_stopwords.txt').read()
# print(f)

# print('hello "word {}"{}'.format('hanfeihu', 'zhouqizhong'))

# import os
# import jieba
# import jieba.posseg as psg
# # 结巴分词
# abspath = os.path.dirname(os.path.abspath(__name__))
# userDict_path = os.path.join(abspath, "data/data_dicts/user_dict.txt")
# jieba.load_userdict(userDict_path)  # 添加自定义词库
# input_string = "理财期限大于等于1.32%的理财产品, 这里是北京"
# seg = jieba.posseg.cut(input_string)
# l = []
# for i in seg:
#     l.append((i.word, i.flag))
# print(l)


# from user_portrait import add_topic_field
from utils import MovieDataApp
#
spark = MovieDataApp().spark
# tmp = add_topic_field()
#
# tmp = spark.sql("select * from movie.db_asset").show()
# spark.sql("select * from movie_portrait.movie_feature").show()
# spark.sql("select * from {}.movie_feature".format('movie_portrait')).show()
# spark.catalog.listTables("movie")
# tmp.filter('cate_id = 1969').show(200)
# tmp.printSchema()
# tmp.sample(False,0.5,0).show(100)
import pyspark.sql.functions as fn
# tmp.groupBy('cid').agg({'cid':'count'}).show()

# from datetime import datetime
# dateArray = datetime.fromtimestamp(1562947200)
# t = datetime.now() - datetime.strptime(dateArray.strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")
# print(t)