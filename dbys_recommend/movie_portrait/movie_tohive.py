from movie_portrait import pre_movie_data, get_tfidf, get_topK_tfidf, idf_textrank, get_movie_profile, \
    get_topK_weight, tfidf_textrank, get_topic_weights, get_textrank, get_topK_textrank, cut_words, \
    get_movie_keywords
from utils import RetToHive, MovieCutWords
from utils import MovieDataApp, movie_portrait_db


database_name = movie_portrait_db
spark_app = MovieDataApp().spark


def save_predata():
    # 保存预处理数据    300794
    predata_ret = pre_movie_data()
    predata_table = 'movie_feature'
    RetToHive(spark_app, predata_ret, database_name, predata_table)


def save_own_cut_words():
    movie_data = spark_app.sql("select * from {}.movie_feature".format(movie_portrait_db))
    own_cut_ret = cut_words(movie_data, cut_field='cate')
    own_cut_table = 'cut_cate'
    RetToHive(spark_app, own_cut_ret, database_name, own_cut_table)

    own_cut_ret = cut_words(movie_data, cut_field='actor')
    own_cut_table = 'cut_actor'
    RetToHive(spark_app, own_cut_ret, database_name, own_cut_table)

    own_cut_ret = cut_words(movie_data, cut_field='director')
    own_cut_table = 'cut_director'
    RetToHive(spark_app, own_cut_ret, database_name, own_cut_table)

    own_cut_ret = cut_words(movie_data, cut_field='area')
    own_cut_table = 'cut_area'
    RetToHive(spark_app, own_cut_ret, database_name, own_cut_table)


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
    cutwords_ret = MovieCutWords(cut_field='desc').get_words(sentence_df)
    cutwords_table = 'cut_desc'
    RetToHive(spark_app, cutwords_ret, database_name, cutwords_table)

    cutwords_ret = MovieCutWords(cut_field='title', add_source=True).get_words(sentence_df)
    cutwords_table = 'cut_title'
    RetToHive(spark_app, cutwords_ret, database_name, cutwords_table)


def save_tfidf(refit=True):
    # 保存tfidf结果    16152341
    tfidf_ret = get_tfidf('desc', refit=refit)
    tfidf_table = 'desc_tfidf'
    RetToHive(spark_app, tfidf_ret, database_name, tfidf_table)

    # 保存tfidf结果   6487897
    tfidf_ret = get_tfidf('title', refit=refit)
    tfidf_table = 'title_tfidf'
    RetToHive(spark_app, tfidf_ret, database_name, tfidf_table)

    # 保存tfidf结果   3461529
    tfidf_ret = get_tfidf('cate', refit=refit)
    tfidf_table = 'cate_tfidf'
    RetToHive(spark_app, tfidf_ret, database_name, tfidf_table)

    # 保存tfidf结果   1046397
    tfidf_ret = get_tfidf('actor', refit=refit)
    tfidf_table = 'actor_tfidf'
    RetToHive(spark_app, tfidf_ret, database_name, tfidf_table)

    # 保存tfidf结果   223544
    tfidf_ret = get_tfidf('director', refit=refit)
    tfidf_table = 'director_tfidf'
    RetToHive(spark_app, tfidf_ret, database_name, tfidf_table)

    # 保存tfidf结果   297616
    tfidf_ret = get_tfidf('area', refit=refit)
    tfidf_table = 'area_tfidf'
    RetToHive(spark_app, tfidf_ret, database_name, tfidf_table)


# def save_topK_tfidf():
#     # 保存topK_tfidf结果    3330241
#     topK = 30
#     topK_tfidf_ret = get_topK_tfidf(topK)
#     topK_tfidf_table = 'topK_tfidf'
#     RetToHive(spark_app, topK_tfidf_ret, database_name, topK_tfidf_table)


def save_topK_idf_textrank():
    # 保存topK_idf_textrank结果    7085944
    topK = 20
    idf_textrank_ret = idf_textrank()
    topK_idf_textrank_ret = get_topK_weight(idf_textrank_ret, topK)
    topK_idf_textrank_table = 'topK_idf_textrank'
    RetToHive(spark_app, topK_idf_textrank_ret, database_name, topK_idf_textrank_table)


def save_topK_tfidf_textrank():
    # 保存topK_tfidf_textrank结果    7085944
    topK = 20
    tfidf_textrank_ret = tfidf_textrank()
    topK_tfidf_textrank_ret = get_topK_weight(tfidf_textrank_ret, topK)
    topK_tfidf_textrank_table = 'topK_tfidf_textrank'
    RetToHive(spark_app, topK_tfidf_textrank_ret, database_name, topK_tfidf_textrank_table)


def save_topic_weights():
    # 保存topic_weights结果   6824117
    topic_weights_ret = get_topic_weights()
    topic_weights_table = 'topic_weights'
    RetToHive(spark_app, topic_weights_ret, database_name, topic_weights_table)


def save_movie_profile():
    # 保存movie_profile结果    3798030
    movie_profile_ret = get_movie_profile()
    movie_profile_table = 'movie_profile'
    RetToHive(spark_app, movie_profile_ret, database_name, movie_profile_table)

def save_movie_keywords():
    # 保存movie_keywords结果   1365065
    movie_keywords_ret = get_movie_keywords()
    movie_keywords_table = 'movie_keywords'
    RetToHive(spark_app, movie_keywords_ret, database_name, movie_keywords_table)


def save_all_field_tfidf():
    # 保存tfidf结果    25425849
    tfidf_ret = get_tfidf('all_field', refit=True)
    tfidf_table = 'all_field_tfidf'
    RetToHive(spark_app, tfidf_ret, database_name, tfidf_table)


def save_all_field_topK_tfidf():
    # 保存topK_tfidf结果    19262840
    topK = 30
    topK_tfidf_ret = get_topK_tfidf('all_field', topK)
    topK_tfidf_table = 'topK_all_field_tfidf'
    RetToHive(spark_app, topK_tfidf_ret, database_name, topK_tfidf_table)


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
    # save_movie_profile()
    # save_movie_keywords()
    # save_all_field_tfidf()
    save_all_field_topK_tfidf()
    pass
