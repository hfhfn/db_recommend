from user_portrait import SaveUserProfile
from user_profile_recall import save_inverted_table, SaveUserRecall
from movie_recall import SaveMovieRecall
from movie_portrait import save_topic_weights_normal, save_predata, save_textrank, save_cut_words, save_tfidf, \
    save_topK_idf_textrank, save_topK_tfidf_textrank, save_keyword_weights, save_topic_words, save_movie_profile, \
    save_topic_weights, get_cv_idf_model
from stat_factor import save_movie_hot_sort, save_movie_hot_factor, save_movie_time, save_movie_year_factor, \
                save_movie_score_factor
from user_action_recall import SaveUserSimilarRecall
from utils import user_recall_db
from update_scheduler import Update


def movie_protrait_run():
    # save_predata()
    save_cut_words()
    get_cv_idf_model()    # 保存cv和idf模型，基于全量数据，需定期更新
    # save_textrank()
    # save_tfidf()
    # save_topK_idf_textrank()
    # save_topK_tfidf_textrank()
    # save_keyword_weights()
    # save_topic_words()
    # save_movie_profile()
    # save_topic_weights()
    # save_topic_weights_normal()


def filter_factor_run(cate_id):
    save_movie_hot_sort()
    save_movie_hot_factor()
    save_movie_score_factor()
    save_movie_time()
    save_movie_year_factor(cate_id)
    pass


def movie_recall_run(channel, cate_id):
    mr = SaveMovieRecall(channel=channel, cate_id=cate_id)
    # mr.save_movie_vector()  # 有默认参数refit为True，默认重新训练模型，False为集群加载已训练好的模型
    # mr.save_bkmeans_cluster()  # 有默认参数refit为True， 默认重新训练模型，False为集群加载已训练好的模型
    # mr.save_cos_similar(start_group=0)  # 有默认参数start_group=0， 中间报错可以接着序号运行，修改start_group参数就好
    # mr.save_movie_recall()
    # mr.save_movie_filter_version_recall()
    # mr.save_movie_filter_hot_score_year()
    mr.save_movie_latest_recall()


def user_profile_run():
    up = SaveUserProfile()
    Update().update_user_history(update=False, cal_history=False)
    up.save_action_weight()
    # up.save_action_weight_normal()
    # up.save_action_topic_weight()  # 基于 topic_weights
    # up.save_action_topic_sort()
    # up.save_user_profile()


def user_profile_recall_run(cate_id):
    save_inverted_table(cate_id)  # 基于 topic_weights
    ur = SaveUserRecall(cate_id)
    # ur.save_pre_user_recall()
    # ur.save_pre2_user_recall()
    # ur.save_pre3_user_recall()
    # ur.save_user_recall()
    ur.save_user_tmp_recall_topK()
    ur.save_user_filter_history_recall()
    ur.save_user_filter_version_recall()
    ur.save_user_recall_hot_score_year_factor()
    ur.save_user_profile_latest_recall()
    # # 单独生成cate_history, 用于导入mysql作为测试的历史行为
    # ur.save_user_history_cate()
    # # 以下两个方法基于 merge_action   一般只需要在更新用户历史数据时重新运行，统计行为数据
    # ur.save_action_stat()
    # ur.save_action_stat_cate()


def user_similar_recall_run(cate_id):
    usr = SaveUserSimilarRecall(cate_id)
    # usr.save_user_similar_recall()
    usr.save_filter_same_recall()
    usr.save_filter_history_recall()
    usr.save_user_similar_latest_recall()



if __name__ == '__main__':
    # 除了上面基于 merge_action 的 3个方法， 其他的方法遵循从上往下的继承链， 前面的改变需要重新运行后面的方法
    # movie_protrait_run()
    # filter_factor_run()
    # movie_recall_run()
    user_profile_run()
    # user_profile_recall_run()
    # user_similar_recall_run()
    pass
