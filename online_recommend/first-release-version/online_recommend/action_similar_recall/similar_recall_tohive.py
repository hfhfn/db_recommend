from utils import RetToHive, cate_id, u_spark, user_recall_db, u_topK, user_portrait_db
from action_similar_recall import UserSimilarRecall

class SaveUserSimilarRecall(object):
    spark_app = u_spark
    user_recall = UserSimilarRecall(user_portrait_db, user_recall_db, spark_app)
    database_name = user_recall_db
    cate_id = cate_id
    topK = u_topK

    def save_user_similar_recall(self):
        # 保存用户行为电影相似度召回结果
        recall_ret = self.user_recall.get_user_similar_recall()
        recall_table = 'user_similar_recall_{}'.format(self.cate_id)
        RetToHive(self.spark_app, recall_ret, self.database_name, recall_table)

    def save_filter_same_recall(self):
        # 综合用户行为电影相似度召回相同的结果
        recall_ret = self.user_recall.get_filter_same_recall()
        recall_table = 'user_similar_filter_same_recall_{}'.format(self.cate_id)
        RetToHive(self.spark_app, recall_ret, self.database_name, recall_table)

    def save_filter_history_recall(self):
        # 过滤用户历史数据
        recall_ret = self.user_recall.get_filter_history_recall()
        recall_table = 'user_similar_filter_history_recall_{}'.format(self.cate_id)
        RetToHive(self.spark_app, recall_ret, self.database_name, recall_table)

    def save_user_similar_latest_recall(self):
        # 保存topK个召回结果
        recall_ret = self.user_recall.get_user_similar_latest_recall()
        recall_table = 'user_similar_recall_{}_{}'.format(self.cate_id, self.topK)
        RetToHive(self.spark_app, recall_ret, self.database_name, recall_table)



if __name__ == '__main__':

    pass
