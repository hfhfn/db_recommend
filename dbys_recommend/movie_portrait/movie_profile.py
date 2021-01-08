from utils import MovieDataApp, movie_portrait_db


spark = MovieDataApp().spark

def get_topic_weights():
    """
    返回主题词， （textrank join tfidf）
    :return: 【dataframe】
    """
    topic_sql = """
                    select t.movie_id, t.cate_id, t.title, t.keyword topic, t.weight tfidf_tr, r.weight idf_tr 
                    from {}.topK_tfidf_textrank t 
                    inner join 
                    {}.topK_idf_textrank r
                    where t.keyword=r.keyword 
                    and 
                    t.movie_id=r.movie_id
                    """.format(movie_portrait_db, movie_portrait_db)
    topic_weight = spark.sql(topic_sql)

    import time
    import datetime
    today = datetime.date.today()
    _yesterday = today - datetime.timedelta(days=1)
    # yesterday = int(time.mktime(time.strptime(str(_yesterday), '%Y-%m-%d')))  # 零点时间戳
    yesterday = _yesterday.strftime("%Y%m%d")

    import pyspark.sql.functions as F
    topic_weight = topic_weight.withColumn('timestamp', F.lit(yesterday))

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


def get_movie_keywords():
    movie_df = spark.sql("select id movie_id, cid cate_id, title, year, score, create_time from {}.movie_feature"
                              .format(movie_portrait_db))

    title_words = spark.sql("select movie_id, words title_words from {}.cut_title".format(movie_portrait_db))

    desc_words = spark.sql("select movie_id, words desc_words from {}.cut_desc".format(movie_portrait_db))

    cate_words = spark.sql("select movie_id, words cate_words from {}.cut_cate".format(movie_portrait_db))

    actor_words = spark.sql("select movie_id, words actor_words from {}.cut_actor".format(movie_portrait_db))

    director_words = spark.sql("select movie_id, words director_words from {}.cut_director".format(movie_portrait_db))

    area_words = spark.sql("select movie_id, words area_words from {}.cut_area".format(movie_portrait_db))

    words_df = movie_df.join(title_words, on='movie_id', how='left') \
                            .join(desc_words, on='movie_id', how='left') \
                            .join(cate_words, on='movie_id', how='left') \
                            .join(actor_words, on='movie_id', how='left') \
                            .join(director_words, on='movie_id', how='left') \
                            .join(area_words, on='movie_id', how='left')

    def map(row):
        words = row.title_words + row.desc_words + row.cate_words + row.actor_words + row.director_words + row.area_words

        return row.movie_id, row.cate_id, row.title, row.year, row.score, words, row.create_time

    words_df = words_df.rdd.map(map).toDF(['movie_id', 'cate_id', 'title', 'year', 'score', 'words', 'create_time'])

    return words_df


def get_movie_profile():
    movie_df = spark.sql("select id movie_id, cid cate_id, title, year, score, create_time from {}.movie_feature"
                              .format(movie_portrait_db))

    title_weights = spark.sql("select movie_id, collect_list(keyword) title_keywords, \
                              collect_list(tfidf) title_weights from {}.title_tfidf group by movie_id"
                              .format(movie_portrait_db))

    cate_weights = spark.sql("select movie_id, collect_list(keyword) cate_keywords, \
                              collect_list(tfidf) cate_weights from {}.cate_tfidf group by movie_id"
                              .format(movie_portrait_db))

    actor_weights = spark.sql("select movie_id, collect_list(keyword) actor_keywords, \
                              collect_list(tfidf) actor_weights from {}.actor_tfidf group by movie_id"
                              .format(movie_portrait_db))

    director_weights = spark.sql("select movie_id, collect_list(keyword) director_keywords, \
                              collect_list(tfidf) director_weights from {}.director_tfidf group by movie_id"
                              .format(movie_portrait_db))

    area_weights = spark.sql("select movie_id, collect_list(keyword) area_keywords, \
                              collect_list(tfidf) area_weights from {}.area_tfidf group by movie_id"
                              .format(movie_portrait_db))

    topic_weights = spark.sql("select movie_id, collect_list(topic) topics, \
                                collect_list(idf_tr) weights from {}.topic_weights group by movie_id"
                              .format(movie_portrait_db))

    weights_df = movie_df.join(title_weights, on='movie_id', how='left') \
                            .join(cate_weights, on='movie_id', how='left') \
                            .join(actor_weights, on='movie_id', how='left') \
                            .join(director_weights, on='movie_id', how='left') \
                            .join(area_weights, on='movie_id', how='left') \
                            .join(topic_weights, on='movie_id', how='left')

    def _func(row):
        cate_TOPK = 10
        other_TOPK = 5
        title_words, area_words, cate_words, actor_words, director_words, topics = {},{},{},{},{},{}
        try:
            title_words = list(zip(row.title_keywords, row.title_weights))
            _ = sorted(title_words, key=lambda x: x[1], reverse=True)
            title_words = dict(_[:other_TOPK])
        except:
            pass
        try:
            area_words = list(zip(row.area_keywords, row.area_weights))
            _ = sorted(area_words, key=lambda x: x[1], reverse=True)
            area_words = dict(_[:other_TOPK])
        except:
            pass
        try:
            cate_words = list(zip(row.cate_keywords, row.cate_weights))
            _ = sorted(cate_words, key=lambda x: x[1], reverse=True)
            cate_words = dict(_[:cate_TOPK])
        except:
            pass
        try:
            actor_words = list(zip(row.actor_keywords, row.actor_weights))
            _ = sorted(actor_words, key=lambda x: x[1], reverse=True)
            actor_words = dict(_[:other_TOPK])
        except:
            pass
        try:
            director_words = list(zip(row.director_keywords, row.director_weights))
            _ = sorted(director_words, key=lambda x: x[1], reverse=True)
            director_words = dict(_[:other_TOPK])
        except:
            pass
        try:
            topics = dict(zip(row.topics, row.weights))
        except:
            pass

        return row.movie_id, row.cate_id, row.title, row.year, row.score, row.create_time, \
            title_words, area_words, cate_words, actor_words, director_words, topics

    weights_df = weights_df.rdd.map(_func).toDF(['movie_id', 'cate_id', 'title', 'year', 'score', 'create_time',
                                                 'title_words', 'area_words', 'cate_words', 'actor_words',
                                                 'director_words', 'topics'])

    return weights_df



if __name__ == '__main__':
    get_topic_weights().show()
