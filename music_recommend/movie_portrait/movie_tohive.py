from movie_portrait import pre_movie_data, get_tfidf, get_topK_tfidf, idf_textrank, \
    get_topK_weight, tfidf_textrank, get_topic_weights, get_textrank, cut_words, \
    get_movie_keywords
from utils import RetToHive, MovieCutWords
from utils import MovieDataApp, movie_portrait_db


database_name = movie_portrait_db
spark_app = MovieDataApp().spark


def save_predata():
    # 保存预处理数据   1149913
    predata_ret = pre_movie_data()
    predata_table = 'movie_feature'
    RetToHive(spark_app, predata_ret, database_name, predata_table)


def save_own_cut_words():
    # 1149913
    own_cut_ret = cut_words()
    own_cut_table = 'first_cut_field'
    RetToHive(spark_app, own_cut_ret, database_name, own_cut_table)

def save_textrank():
    # 保存textrank结果    18297132
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
    # 保存分词结果    1149913
    sentence_df = spark_app.sql("select * from {}.first_cut_field".format(database_name))
    cutwords_ret = MovieCutWords().get_words(sentence_df)
    cutwords_table = 'cut_content'
    RetToHive(spark_app, cutwords_ret, database_name, cutwords_table)

    # cutwords_ret = MovieCutWords(cut_field='title', add_source=True).get_words(sentence_df)
    # cutwords_table = 'cut_title'
    # RetToHive(spark_app, cutwords_ret, database_name, cutwords_table)


def save_tfidf(refit=True):
    # 保存tfidf结果    27116634
    tfidf_ret = get_tfidf('content', refit=refit)
    tfidf_table = 'content_tfidf'
    RetToHive(spark_app, tfidf_ret, database_name, tfidf_table)


# def save_topK_tfidf():
#     # 保存topK_tfidf结果    3330241
#     topK = 30
#     topK_tfidf_ret = get_topK_tfidf(topK)
#     topK_tfidf_table = 'topK_tfidf'
#     RetToHive(spark_app, topK_tfidf_ret, database_name, topK_tfidf_table)


def save_topK_idf_textrank():
    # 保存topK_idf_textrank结果    11874907
    topK = 30
    idf_textrank_ret = idf_textrank()
    topK_idf_textrank_ret = get_topK_weight(idf_textrank_ret, topK)
    topK_idf_textrank_table = 'topK_idf_textrank'
    RetToHive(spark_app, topK_idf_textrank_ret, database_name, topK_idf_textrank_table)


def save_topK_tfidf_textrank():
    # 保存topK_tfidf_textrank结果    12447879
    topK = 30
    tfidf_textrank_ret = tfidf_textrank()
    topK_tfidf_textrank_ret = get_topK_weight(tfidf_textrank_ret, topK)
    topK_tfidf_textrank_table = 'topK_tfidf_textrank'
    RetToHive(spark_app, topK_tfidf_textrank_ret, database_name, topK_tfidf_textrank_table)


def save_topic_weights():
    # 保存topic_weights结果   6824117
    topic_weights_ret = get_topic_weights()
    topic_weights_table = 'topic_weights'
    RetToHive(spark_app, topic_weights_ret, database_name, topic_weights_table)


# def save_song_profile():
#     # 保存movie_profile结果    3798030
#     movie_profile_ret = get_movie_profile()
#     movie_profile_table = 'song_profile'
#     RetToHive(spark_app, movie_profile_ret, database_name, movie_profile_table)

def save_song_keywords():
    # 保存movie_keywords结果   1149913
    movie_keywords_ret = get_movie_keywords()
    movie_keywords_table = 'song_keywords'
    RetToHive(spark_app, movie_keywords_ret, database_name, movie_keywords_table)


# def save_topic_weights_normal():
#     # 保存归一化topic_weights结果    3798030
#     topic_weights_ret = normal_topic_weights()
#     topic_weights_table = 'topic_weights_normal'
#     RetToHive(spark_app, topic_weights_ret, database_name, topic_weights_table)


# def change_field():     #  临时用
#     tmp_ret = spark_app.sql('select * from pre_user.merge_play')\
#                 .withColumnRenamed("datetime", "data_time").drop("data_time")
#     tmp_table = 'merge_play'
#     RetToHive(spark_app, tmp_ret, 'pre_user', tmp_table)
# change_field()

if __name__ == '__main__':
    # save_predata()
    # save_own_cut_words()
    # save_textrank()
    # save_cut_words()
    # save_tfidf()
    # save_topK_idf_textrank()
    # save_topK_tfidf_textrank()
    # save_topic_weights()
    save_song_keywords()
    pass
