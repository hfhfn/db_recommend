from utils import MovieDataApp, movie_portrait_db
from movie_portrait.cal_merge_weight import idf_textrank


spark = MovieDataApp().spark

def get_keyword_weights():
    """
    返回关键词和权重
    :return: 【dataframe】
    """
    rt = idf_textrank()
    rt.registerTempTable("temptable")
    merge_keywords = spark.sql("select movie_id, min(cate_id) cate_id, collect_list(keyword) keywords, \
                               collect_list(weights) weights from temptable group by movie_id")

    # 合并关键词权重合并成字典
    def _func(row):
        return row.movie_id, row.cate_id, dict(zip(row.keywords, row.weights))
    keywords_info = merge_keywords.rdd.map(_func).toDF(["movie_id", "cate_id", "keywords"])

    return keywords_info

def get_topic_words():
    """
    返回主题词， （textrank join tfidf）
    :return: 【dataframe】
    """
    topic_sql = """
                    select movie_id, min(cate_id) cate_id, collect_list(keyword) topics 
                    from
                    (select t.movie_id movie_id, r.cate_id cate_id, t.keyword keyword, t.weight tf_r, r.weight idf_r 
                    from {}.topK_tfidf_textrank t 
                    inner join 
                    {}.topK_idf_textrank r
                    where t.keyword=r.keyword 
                    and 
                    t.movie_id=r.movie_id)
                    group by 
                    movie_id
                    """.format(movie_portrait_db, movie_portrait_db)
    movie_topics = spark.sql(topic_sql)

    return movie_topics

def get_profile():
    """
    返回关键词和权重 + 主题词序列
    :return: 【dataframe】
    """
    # keywords_info = get_keyword_weights()
    # movie_topics = get_topic_words()
    spark.sql("use {}".format(movie_portrait_db))
    keywords_info = spark.sql("select * from keyword_weights")
    movie_topics = spark.sql("select movie_id movie_id2, cate_id cate_id2, topics from topic_words")

    movie_profile_ret = keywords_info.join(movie_topics, keywords_info.movie_id==movie_topics.movie_id2)\
        .select(["movie_id", "cate_id", "keywords", "topics"])

    return movie_profile_ret

def get_topic_weights():
    spark.sql("use movie_portrait")

    # 获取影视的画像
    movie_profile = spark.sql("select * from movie_profile")
    # 把影视关键词字典结果，进行展开成两部分，词，权重，两列键
    # LATERAL VIEW explode(keywords)
    movie_profile.registerTempTable("tempTable")
    idf_textrank_weight = spark.sql("select movie_id, cate_id, keyword, weight, topics \
                                        from tempTable LATERAL VIEW explode(keywords) AS keyword, weight")

    import time
    import datetime
    today = datetime.date.today()
    _yesterday = today - datetime.timedelta(days=1)
    yesterday = int(time.mktime(time.strptime(str(_yesterday), '%Y-%m-%d')))  # 零点时间戳
    # 用影视的主题词结果删选关键字结果， 只保留主题词的权重
    import pyspark.sql.functions as F
    # 重新select为了改变列 topic和weight顺序，便于和增量topic_weights表列名顺序一致
    topic_weight = idf_textrank_weight.withColumn('topic', F.explode('topics')).drop('topics')\
                        .filter('keyword=topic').drop('keyword').select(['movie_id', 'cate_id', 'topic', 'weight'])\
                        .withColumn('timestamp', F.lit(yesterday))
    return topic_weight

# def normal_topic_weights():
#     topic_weights = spark.sql("select * from movie_portrait.topic_weights")
#     import pyspark.sql.functions as fn
#     tmp = topic_weights.agg(fn.max('weight').alias('max'),fn.min('weight').alias('min')).first()
#     # 归一化
#     min = tmp.min
#     # print(min)
#     max = tmp.max
#     # print(max)
#     def normal(row):
#         weight = (row.weight - min) / (max - min)
#         return row.movie_id, row.cate_id, row.topic, round(float(weight), 8)
#
#     df = topic_weights.rdd.map(normal).toDF(['movie_id', 'cate_id', 'topic', 'weight'])
#     title_df = spark.sql("select id, cid, title from movie.db_asset")
#     df = df.join(title_df, (df.cate_id == title_df.cid) & (df.movie_id == title_df.id),
#                                how='left').drop('id', 'cid')
#     return df

if __name__ == '__main__':
    # get_profile().show()
    # get_topic_words().show(truncate=False)
    get_topic_weights().show()
