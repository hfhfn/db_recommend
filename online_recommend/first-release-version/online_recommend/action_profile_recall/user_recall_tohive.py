from utils import RetToHive, cate_id, u_spark, user_recall_db, pre_topK, u_topK, user_portrait_db, user_pre_db
from action_profile_recall import get_inverted_table, UserRecall, UserFilterRecall


def save_inverted_table():
    # 保存电影画像倒排表    186470
    # movie_li字段是以 str类型保存的list
    inverted_ret = get_inverted_table(cate_id)
    inverted_table = 'inverted_table_{}'.format(cate_id)
    RetToHive(u_spark, inverted_ret, user_recall_db, inverted_table)


class SaveUserRecall(object):
    spark_app = u_spark
    user_recall = UserRecall(user_portrait_db, user_recall_db, spark_app)
    user_filter_recall = UserFilterRecall(user_pre_db, user_recall_db, spark_app)
    database_name = user_recall_db
    pre_topK = pre_topK
    topK = u_topK
    cate_id = cate_id

    def save_pre_user_recall(self):
        # 由于 内存GC 原因， 拆分为3步召回电影
        # 保存第一步召回结果    2896745
        recall_ret = self.user_recall.get_pre_recall()
        recall_table = 'pre_user_recall_{}'.format(self.cate_id)
        RetToHive(self.spark_app, recall_ret, self.database_name, recall_table)

    def save_pre2_user_recall(self):
        # 保存第二步召回结果    320873061
        recall_ret = self.user_recall.get_pre2_recall()
        recall_table = 'pre2_user_recall_{}'.format(self.cate_id)
        RetToHive(self.spark_app, recall_ret, self.database_name, recall_table)

    def save_pre3_user_recall(self):
        # 保存第三步召回结果    320873061
        recall_ret = self.user_recall.get_pre3_recall()
        recall_table = 'pre3_user_recall_{}'.format(self.cate_id)
        RetToHive(self.spark_app, recall_ret, self.database_name, recall_table)

    def save_user_recall(self):
        # 保存召回结果    250902000
        recall_ret = self.user_recall.get_recall_ret()
        recall_table = 'user_recall_{}'.format(self.cate_id)
        RetToHive(self.spark_app, recall_ret, self.database_name, recall_table)

    def save_user_tmp_recall_topK(self):
        # 先做一波召回，减少计算量      59132322
        recall_topK = self.user_recall.get_tmp_recall_topK()
        recall_topK_table = 'user_recall_{}_{}'.format(self.cate_id, self.pre_topK)
        RetToHive(self.spark_app, recall_topK, self.database_name, recall_topK_table)

    def save_user_filter_history_recall(self):
        # 保存过滤了历史观看和点击记录的召回结果    58540632
        filter_recall_ret = self.user_filter_recall.get_filter_history_recall()
        filter_recall_table = 'user_filter_history_recall_{}'.format(self.cate_id)
        RetToHive(self.spark_app, filter_recall_ret, self.database_name, filter_recall_table)

    def save_user_filter_version_recall(self):
        # 保存过滤了同名电影的不同版本或花絮的召回结果     48142763
        filter_recall_ret = self.user_filter_recall.get_filter_version_recall()
        filter_recall_table = 'user_filter_version_recall_{}'.format(self.cate_id)
        RetToHive(self.spark_app, filter_recall_ret, self.database_name, filter_recall_table)

    def save_user_recall_hot_score_year_factor(self):
        # 保存 使用电影年代加权的召回结果    48142763
        recall_ret = self.user_recall.get_user_recall_hot_score_year_factor()
        recall_table = 'user_recall_factor_{}'.format(self.cate_id)
        RetToHive(self.spark_app, recall_ret, self.database_name, recall_table)

    def save_user_profile_latest_recall(self):
        # 保存topK个召回结果    100 -> 6289280
        recall_topK = self.user_filter_recall.get_user_latest_recall()
        recall_topK_table = 'user_profile_recall_{}_{}'.format(self.cate_id, self.topK)
        RetToHive(self.spark_app, recall_topK, self.database_name, recall_topK_table)

    def save_action_stat(self):
        # 保存用户行为数据的统计结果    282091
        action_stat_ret = self.user_filter_recall.get_action_stat()
        action_stat_table = 'action_stat'
        RetToHive(self.spark_app, action_stat_ret, self.database_name, action_stat_table)

    def save_action_stat_cate(self):
        # 分类保存用户行为数据的统计结果    1969 => 62966
        action_stat_ret = self.spark_app.sql("select * from {}.action_stat".format(self.database_name)).where(
            'cate_id = {}'.format(self.cate_id)).drop('cate_id')
        action_stat_table = 'action_stat_{}'.format(self.cate_id)
        RetToHive(self.spark_app, action_stat_ret, self.database_name, action_stat_table)


if __name__ == '__main__':
    # save_inverted_table()
    ur = SaveUserRecall()
    ur.save_user_recall()
    # save_user_recall_year_factor()
    # save_user_recall_topK()
    # save_user_history()
    # save_user_history_cate()
    # save_user_mac()
    # save_merge_mac()
    # save_user_filter_recall()
    # save_action_stat()
    # save_action_stat_cate()
    pass

# def save_user_mac():
#     # 保存用户设备id和mac的对应结果
#     # table_type = 'click'
#     # table_type = 'top'
#     table_type = 'play'
#     user_mac_ret = get_user_mac(table_type)
#     user_mac_table = 'user_mac_{}'.format(table_type)
#     RetToHive(spark_app, user_mac_ret, database_name, user_mac_table)
#
# def save_merge_mac():
#     # 保存用户设备id和mac的对应结果    54662
#     user_mac_ret = merge_user_mac()
#     user_mac_table = 'user_mac'
#     RetToHive(spark_app, user_mac_ret, database_name, user_mac_table)
