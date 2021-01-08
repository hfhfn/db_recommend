from utils import MovieDataApp, movie_portrait_db


spark = MovieDataApp().spark

def get_topic_weights():
    """
    返回主题词， （textrank join tfidf）
    :return: 【dataframe】
    """
    topic_sql = """
                    select t.song_id, t.song, t.keyword topic, t.weight tfidf_tr, r.weight idf_tr 
                    from {}.topK_tfidf_textrank t 
                    inner join 
                    {}.topK_idf_textrank r
                    where t.keyword=r.keyword 
                    and 
                    t.song_id=r.song_id
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
    song_df = spark.sql("select song_id, song, singer, album from {}.first_cut_field"
                              .format(movie_portrait_db))

    content_words = spark.sql("select song_id, collect_list(topic) topic from {}.topic_weights group by song_id"
                              .format(movie_portrait_db))

    song_df = song_df.join(content_words, on='song_id', how='left')

    def map(row):
        try:
            tags = row.song + row.singer + row.album + row.topic
        except:
            # topic为空
            tags = row.song + row.singer + row.album
        return row.song_id, ','.join(row.song), ','.join(row.singer), ','.join(row.album), str(tags)

    words_df = song_df.rdd.map(map).toDF(['song_id', 'song', 'singer', 'album', 'tags'])

    return words_df



if __name__ == '__main__':
    get_topic_weights().show()
