from movie_protrait import pre_movie_data, MovieCutWords, get_tfidf, get_topK_tfidf, \
    idf_textrank, get_topK_weight, tfidf_textrank, get_keyword_weights, get_topic_words, \
    get_profile, get_topic_weights, get_textrank, get_topK_textrank, normal_topic_weights

from utils import RetToHive
from utils import MovieDataApp, movie_protrait_db

database_name = movie_protrait_db
spark_app = MovieDataApp().spark


def save_predata():
    # 保存预处理数据    300794
    predata_ret = pre_movie_data()
    predata_table = 'movie_feature'
    RetToHive(spark_app, predata_ret, database_name, predata_table)


def save_textrank():
    # 保存textrank结果    4853793
    textrank_ret = get_textrank()
    textrank_table = 'textrank'
    RetToHive(spark_app, textrank_ret, database_name, textrank_table)

# def save_topK_textrank():
#     # 保存topK_textrank结果    3297530
#     topK = 30
#     topK_textrank_ret = get_topK_textrank(topK)
#     topK_textrank_table = 'topK_textrank'
#     RetToHive(spark_app, topK_textrank_ret, database_name, topK_textrank_table)


def save_cut_words():
    # 保存分词结果    300794
    sentence_df = spark_app.sql("select * from {}.movie_feature".format(database_name))
    cutwords_ret = MovieCutWords().get_words(sentence_df)
    cutwords_table = 'cut_words'
    RetToHive(spark_app, cutwords_ret, database_name, cutwords_table)


def save_tfidf():
    # 保存tfidf结果    7290336
    tfidf_ret = get_tfidf()
    tfidf_table = 'tfidf'
    RetToHive(spark_app, tfidf_ret, database_name, tfidf_table)


# def save_topK_tfidf():
#     # 保存topK_tfidf结果    3330241
#     topK = 30
#     topK_tfidf_ret = get_topK_tfidf(topK)
#     topK_tfidf_table = 'topK_tfidf'
#     RetToHive(spark_app, topK_tfidf_ret, database_name, topK_tfidf_table)


def save_topK_idf_textrank():
    # 保存topK_idf_textrank结果    3981638
    topK = 30
    idf_textrank_ret = idf_textrank()
    topK_idf_textrank_ret = get_topK_weight(idf_textrank_ret, topK)
    topK_idf_textrank_table = 'topK_idf_textrank'
    RetToHive(spark_app, topK_idf_textrank_ret, database_name, topK_idf_textrank_table)


def save_topK_tfidf_textrank():
    # 保存topK_tfidf_textrank结果    3981638
    topK = 30
    tfidf_textrank_ret = tfidf_textrank()
    topK_tfidf_textrank_ret = get_topK_weight(tfidf_textrank_ret, topK)
    topK_tfidf_textrank_table = 'topK_tfidf_textrank'
    RetToHive(spark_app, topK_tfidf_textrank_ret, database_name, topK_tfidf_textrank_table)


def save_keyword_weights():
    # 保存keyword_weights结果    296056
    keyword_weights_ret = get_keyword_weights()
    keyword_weights_table = 'keyword_weights'
    RetToHive(spark_app, keyword_weights_ret, database_name, keyword_weights_table)


def save_topic_words():
    # 保存topic_words结果    296056
    topic_words_ret = get_topic_words()
    topic_words_table = 'topic_words'
    RetToHive(spark_app, topic_words_ret, database_name, topic_words_table)


def save_movie_profile():
    # 保存movie_profile结果    296056
    movie_profile_ret = get_profile()
    movie_profile_table = 'movie_profile'
    RetToHive(spark_app, movie_profile_ret, database_name, movie_profile_table)


def save_topic_weights():
    # 保存topic_weights结果    3798030
    topic_weights_ret = get_topic_weights()
    topic_weights_table = 'topic_weights'
    RetToHive(spark_app, topic_weights_ret, database_name, topic_weights_table)


def save_topic_weights_normal():
    # 保存归一化topic_weights结果    3798030
    topic_weights_ret = normal_topic_weights()
    topic_weights_table = 'topic_weights_normal'
    RetToHive(spark_app, topic_weights_ret, database_name, topic_weights_table)


# def change_field():     #  临时用
#     tmp_ret = spark_app.sql('select * from pre_user.merge_play')\
#                 .withColumnRenamed("datetime", "data_time").drop("data_time")
#     tmp_table = 'merge_play'
#     RetToHive(spark_app, tmp_ret, 'pre_user', tmp_table)
# change_field()

if __name__ == '__main__':
    # save_predata()
    save_textrank()
    # save_top20_textrank()
    # save_cut_words()
    save_tfidf()
    # save_top20_tfidf()
    # save_top20_idf_textrank()
    # save_top20_tfidf_textrank()
    # save_keyword_weights()
    # save_topic_words()
    # save_movie_profile()
    # save_topic_weights()
    # save_movie_time()
    # save_movie_year_factor()
    pass