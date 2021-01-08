# spark.catalog.listDatabases()


# import pyspark.sql.functions as fn
# from pyspark.sql import Window
#
# sec_movie = fir_movie.withColumn("row_number",
#                                  fn.row_number().over(Window.partitionBy("cid").orderBy(fir_movie["id"].asc())))
# fir_movie.withColumn("row_number", fn.row_number().over(Window.partitionBy("cid").orderBy(fn.desc('id')))).show()


# movieid和各个特征合并
# sql = '''
# select id, cid, concat_ws(",", collect_set(fir_movie)) as summary
# (select title, desc, cate, year, area, actor, director, score, all_count, update_time from movie_data as fir_movie)
# '''
# # 选取movie表中需要保留的数据
# spark.sql(sql).show()


# pre_movie_data().registerTempTable("tempTable")
# # 查看临时表结构
# spark.sql("desc tempTable").show()

"""
不同模块废弃代码分割线
"""

# def get_keyword_weights():
#     """
#     返回关键词和权重
#     :return: 【dataframe】
#     """
#     rt = idf_textrank()
#     rt.registerTempTable("temptable")
#     merge_keywords = spark.sql("select movie_id, min(cate_id) cate_id, collect_list(keyword) keywords, \
#                                collect_list(weights) weights from temptable group by movie_id")
#
#     # 合并关键词权重合并成字典
#     def _func(row):
#         return row.movie_id, row.cate_id, dict(zip(row.keywords, row.weights))
#     keywords_info = merge_keywords.rdd.map(_func).toDF(["movie_id", "cate_id", "keywords"])
#
#     return keywords_info


# def get_profile():
#     """
#     返回关键词和权重 + 主题词序列
#     :return: 【dataframe】
#     """
#     # keywords_info = get_keyword_weights()
#     # movie_topics = get_topic_words()
#     spark.sql("use {}".format(movie_portrait_db))
#     keywords_info = spark.sql("select * from keyword_weights")
#     movie_topics = spark.sql("select movie_id movie_id2, cate_id cate_id2, topics from topic_words")
#
#     movie_profile_ret = keywords_info.join(movie_topics, keywords_info.movie_id==movie_topics.movie_id2)\
#         .select(["movie_id", "cate_id", "keywords", "topics"])
#
#     return movie_profile_ret


# def get_topic_weights():
#     spark.sql("use movie_portrait")
#
#     # 获取影视的画像
#     movie_profile = spark.sql("select * from movie_profile")
#     # 把影视关键词字典结果，进行展开成两部分，词，权重，两列键
#     # LATERAL VIEW explode(keywords)
#     movie_profile.registerTempTable("tempTable")
#     idf_textrank_weight = spark.sql("select movie_id, cate_id, keyword, weight, topics \
#                                         from tempTable LATERAL VIEW explode(keywords) AS keyword, weight")
#
#     import time
#     import datetime
#     today = datetime.date.today()
#     _yesterday = today - datetime.timedelta(days=1)
#     yesterday = int(time.mktime(time.strptime(str(_yesterday), '%Y-%m-%d')))  # 零点时间戳
#     # 用影视的主题词结果删选关键字结果， 只保留主题词的权重
#     import pyspark.sql.functions as F
#     # 重新select为了改变列 topic和weight顺序，便于和增量topic_weights表列名顺序一致
#     topic_weight = idf_textrank_weight.withColumn('topic', F.explode('topics')).drop('topics')\
#                         .filter('keyword=topic').drop('keyword').select(['movie_id', 'cate_id', 'topic', 'weight'])\
#                         .withColumn('timestamp', F.lit(yesterday))
#     return topic_weight

"""
不同模块废弃代码分割线
"""




# a = 'djfk, djfia,,kjfakd, , fkdjkfj'
# l = list(set(''.join(a.split()).split(',')))
# l.remove('')
# print(l)


import re

# def tokenlize(sentence):
#
#     fileters = ['!', '"', '#', '$', '%', '&', '\(', '\)', '\*', '\+', ',', '-', '\.', '/', ':', ';', '<', '=', '>',
#                 '\?', '@', '\[', '\\', '\]', '^', '_', '`', '\{', '\|', '\}', '~', '\t', '\n', '\x97', '\x96', '”', '“', ]
#     sentence = sentence.lower() #把大写转化为小写
#     sentence = re.sub("<br />"," ",sentence)
#     # sentence = re.sub("I'm","I am",sentence)
#     # sentence = re.sub("isn't","is not",sentence)
#     sentence = re.sub("|".join(fileters)," ",sentence)
#     result = [i for i in sentence.split(" ") if len(i)>0]
#
#     return result
