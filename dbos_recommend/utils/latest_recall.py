from utils import FilterRecall, LatestFilterRecall, FilterApp, user_recall_db, movie_recall_db

spark = FilterApp().spark

def get_latest_recall(similar, cate_id, recall_db, filter, recall='als'):
    if filter == 'user':
        f_recall = FilterRecall(spark, cate_id, user_recall_db)
        similar = f_recall.get_filter_history_recall(similar)
    else:
        f_recall = FilterRecall(spark, cate_id, movie_recall_db)
    recall_df = f_recall.get_filter_version_recall(similar, filter=filter)
    recall_df = f_recall.get_filter_hot_score_year(recall_df, filter=filter)
    """
    # 在下面方法中，对movie_recall结果进行了 insertInto 更新
    """
    mf_recall = LatestFilterRecall(spark, cate_id, recall_db)
    if filter == 'user':
        recall_df = mf_recall.get_user_latest_recall(recall_df, recall=recall)
    else:
        recall_df = mf_recall.get_movie_latest_recall(recall_df)

    return recall_df