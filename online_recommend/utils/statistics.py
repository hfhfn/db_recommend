from utils.spark_app import MovieDataApp


spark = MovieDataApp().spark

# tmp = spark.sql("select * from user_portrait.action_topic_sort").where('cate_id = 1969')
# tmp = spark.sql("select * from user_recall.user_filter_recall")
# tmp = spark.sql("select * from movie_recall.movie_bkmeans_1969_k50m800")
# tmp = spark.sql("select * from pre_user.merge_action")
# tmp = spark.sql("select * from movie_recall.movie_recall_1969")
# tmp = spark.sql("select * from movie_portrait.topic_weights")
tmp = spark.sql("select * from movie.db_asset")
tmp.show()
# print(tmp.count())
# class_tmp = tmp.where('row_number = 10').count()    # 至少有10个的数量
# print(class_tmp)
# count = tmp.select('user_id').dropna().distinct().count()    # 去重统计数量
# print(count)
# tmp.describe('sort_num').show()    # count, mean, min, max, stddev
# tmp.describe('cos_sim').show()    # count, mean, min, max, stddev
# import pyspark.sql.functions as fn
# tmp.groupby('group').agg(fn.count('group').alias('count')).show(100)     # 分组 count
# tmp_pandas = tmp.toPandas()['row_number']
# describe = tmp_pandas.describe()  #mean, std, min, 25% 50%(median) 75%, max
# print(describe)
# median = tmp_pandas.median()   # 中位数
# print(median)
# mode = tmp_pandas.mode()       # 众数
# print(mode)
# tmp.agg(fn.max('row_number').alias('max'),fn.mean('row_number').alias('mean'),
#         fn.min('row_number').alias('min')).show()    # 同 describe 方法