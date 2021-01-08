import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(BASE_DIR))

from movie_recall.movie_recall_tohive import SaveMovieRecall
from stat_factor.filter_tohive import save_movie_hot_sort, save_movie_hot_factor, save_movie_score_factor, \
    save_movie_year_factor, save_user_history, save_movie_time, save_movie_auth, save_movie_total_num
from content_recall.update_movie_recall import UpdateMovie
from content_recall.update_tohive import SaveUpdateUserSimilarRecall, SaveUpdateUserProfile
from user_portrait.user_tohive import SaveUserProfile
from utils.default import user_pre_db, m_spark, movie_recall_db, interval, \
    update_movie_db, history_year, history_month, history_day
from utils.save_tohive import RetToHive


# def update_cv_idf_model_run():
#     """
#     定期基于全量数据，更新cv和idf模型  ==》 一周更新一次
#     基于全量的cut_words
#     :return:
#     """
#     save_predata()
#     save_cut_words()
#     get_cv_idf_model()


class Update(object):
    spark = m_spark

    def __init__(self, channel='电影', cate_id=1969):
        self.channel = channel
        self.cate_id = cate_id
        self.ua = UpdateMovie(channel, cate_id)

    def update_movie_vector(self, refit=True):
        """
        定期基于全量数据，更新电影向量  ==>  一月 / 一周更新一次
        基于全量的cut_words, topic_weights
        :return:
        """
        mr = SaveMovieRecall(self.channel, self.cate_id)
        mr.save_movie_vector(refit=refit)  # 默认重新训练word2vec模型
        import gc
        del mr
        gc.collect()


    def update_factor(self):
        """
        定期基于全量数据，更新year，score，play_hot因子   ==》  一周更新一次
        :return:
        """
        save_movie_hot_sort()
        save_movie_hot_factor()
        save_movie_score_factor()
        # 综艺 年代衰减不同于其他频道
        save_movie_year_factor(1971)  # 综艺
        save_movie_year_factor(1969)  # 其他，这里用电影代表 非综艺
        save_movie_time()
        save_movie_auth()
        save_movie_total_num()


    def update_user_history(self, full=False, cal_history=True):
        """
        以增量的方式最终实现全量更新
        全量更新用户的历史行为  == 》  每天更新
        :return:
        """
        up = SaveUserProfile()
        import datetime

        today = datetime.date.today()
        yesterday = today - datetime.timedelta(days=1)
        yesterday_str = yesterday.strftime("%Y, %m, %d")
        day = int(yesterday_str[-2:])
        if day <= 10:
            date = yesterday_str[:-2] + '10'
        elif day <= 20:
            date = yesterday_str[:-2] + '20'
        else:
            date = yesterday_str[:-2] + '30'

        # 固定日期， 当贝os 数据采集起始日期
        start_year, start_moon, start_day = history_year, history_month, history_day
        # 默认情况， 增量导入数据起始和结束都是同一个时间，即当前最新时间数据
        table_year, table_moon, table_day = date.split(',')  # 用来表征增量数据起始导入日期
        end_year, end_moon, end_day = date.split(',')  # 用来表征 最近数据日期 即增量数据 截止日期
        table_num = (int(end_year) - int(start_year)) * 12 * 3 + (int(end_moon) - int(start_moon)) * 3 + (
                    int(end_day) - int(start_day)) // 10 + 1
        if not full:
            start_num = (int(table_year) - int(start_year)) * 12 * 3 + (int(table_moon) - int(start_moon)) * 3 + (
                        int(table_day) - int(start_day)) // 10
        else:
            start_num = 0
        action = ['click', 'top', 'play']
        for table_type in action:
            up.save_pre_userdata(table_type, table_num, start_num)
            up.save_merge_userdata(table_type, table_num, start_num)
            if start_num != 0:
                # 用于删除以上方法中零时移用的表
                self.spark.sql('drop table {}.merge_{}_{}'.format(user_pre_db, table_type, start_num))
        up.save_merge_action(start_time=0)  # start_time 默认等于0, 即使用全量的数据  ==>  每天更新
        if cal_history:
            save_user_history()  # 基于全量merge_action， 一般只需要在更新用户历史数据时重新运行
        import gc
        del up
        gc.collect()

# from utils import MovieDataApp
# spark = MovieDataApp().spark
# def tmp_insert_topic_weights():
#
#     spark.sql('INSERT overwrite TABLE movie_portrait.topic_weights SELECT * FROM update_movie.update_movie_profile')
#
#
# def tmp_insert_movie_recall():
#
#     spark.sql('INSERT overwrite TABLE movie_recall.movie_recall_1969_100 SELECT * FROM update_movie.update_movie_recall_1969')


# def update_movie_recall_run():
#     """
#     增量计算电影相似度召回
#     :return:
#     """
#     umr = SaveUpdateMovieRecall()
#     # update_cv_idf_model_run()
#     umr.save_update_movie_profile()   # 基于方法 update_cv_idf_model_run()  得到 topic_weights
#     # tmp_insert_topic_weights()
#     update_movie_vector()    # 默认重新训练word2vec模型
#     """
#     refit = True 重新训练word2vec模型   ==》  一周更新（重新训练）一次
#     默认 refit = False ，方法update_movie_vector()中已经重新训练过word2vec模型
#     """
#     umr.save_update_movie_similar(refit=False)    # 基于方法 update_movie_vector()
#     update_factor()
#     umr.save_update_movie_recall()    # 基于方法 update_factor()
#     # tmp_insert_movie_recall()


    def update_movie_profile(self, full=False):
        """
        更新画像，即 topic_weights表
        :param full:
        :return:
        """
        """
        以下方法中，insert插入表在增量更新时，movie_feature,cut_words,tfidf,textrank这几张表后续不会被引用
        但 最终画像结果表 topic_weights 会被 计算movie_vector时 引用，需要防止同一天重复插入（已处理）
        """
        sentence_df = self.ua.merge_movie_data(full=full)
        # sentence_df.show()
        movie_profile_table = 'update_movie_profile'
        if sentence_df.rdd.count():
            rank, idf = self.ua.generate_movie_label(sentence_df, full=full)
            movieProfile = self.ua.get_movie_profile(rank, idf, full=full)

            """
            保存更新的电影画像数据, 保存此表只是为了判断是否有更新数据，以便于决定是否计算电影相似度，
            else中 如果没有更新数据，则需要清空此表，之后判断此表为空的话，就不进行电影相似度计算
            """
            try:
                movieProfile.write.insertInto('{}.{}'.format(update_movie_db, movie_profile_table), overwrite=True)
            except:
                RetToHive(self.spark, movieProfile, update_movie_db, movie_profile_table)
            import gc
            del rank
            del idf
            del movieProfile
            gc.collect()
        else:
            self.spark.sql('truncate table {}.{}'.format(update_movie_db, movie_profile_table))

        #     # 增量插入保存，返回值未被调用
        #     return movieProfile
        # else:
        #     return sentence_df


    def update_movie_similar(self):
        """
        使用faiss模型计算电影相似度
        :return:
        """
        """
        refit = True 重新训练word2vec模型   ==》  一周更新（重新训练）一次
        默认 refit = False ，方法update_movie_vector()中已经重新训练过word2vec模型
        """
        # 直接传入全量的movieProfile，每次计算全量相似度
        movie_similar = self.ua.compute_movie_similar()

        # 全量覆盖插入保存，返回值未被调用
        return movie_similar


    def update_movie_recall(self, cate_id):
        """
        对相似度召回结果进行过滤
        :return:
        """
        # 读取全量的 movie_similar 数据进行过滤
        movie_similar = self.spark.sql(
            'select * from {}.movie_similar_{}'.format(movie_recall_db, cate_id))
        movie_recall = self.ua.get_filter_latest_recall(movie_similar)

        # 全量覆盖插入保存，返回值未被调用
        return movie_recall


    def update_movie_recall_run(self, flag):
        sign = flag.rdd.count()
        if sign:   # 如果是False 说明movie没有更新数据， 不需要重新计算以下步骤
            # # 得到基于全量数据的 movie_vector，用于 movie_similar 计算
            self.update_movie_vector(refit=True)  # 基于单频道计算
            # 余弦相似度召回
            self.update_movie_similar()  # 基于单频道计算
            # 可选择一周更新，或日更，提出去所有频道只计算一次，否则在此处要多次计算
            # self.update_factor()
            # 得到最终的topK个召回集
            self.update_movie_recall(self.cate_id)


    def update_user_profile_run(self, full=False):
        """
        用户增量数据计算结果都保存在增量数据库 update_user 中
        :return:
        """
        uup = SaveUpdateUserProfile()
        """
        # 需要根据start_time筛选近7天的数据
        # 基于全量的 merge_action, 即 基于方法 update_user_history()
        """
        if full:
            start_time = 0
        else:
            import time
            import datetime
            today = datetime.date.today()
            day_timestamp = int(time.mktime(time.strptime(str(today), '%Y-%m-%d')))  # 零点时间戳
            start_time = day_timestamp - interval

        uup.save_merge_action(start_time)
        uup.save_action_weight()
        uup.save_action_weight_normal()
        """
        # 以上方法 在进行 历史相似度召回和 用户画像召回时 都需要计算
        # 以下方法 只在 用户画像召回时计算
        """
        # uup.save_action_topic_weight()  # 基于 topic_weights
        # uup.save_action_topic_sort()
        # uup.save_user_profile()
        import gc
        del uup
        gc.collect()


    def update_profile_recall_run(self):
        # save_topic_weights_normal()    # 基于 topic_weights
        # save_inverted_table()  # 基于 topic_weights_normal， 基于全量的数据
        # uur = SaveUpdateUserProfileRecall()
        # uur.save_pre_user_recall()
        # uur.save_pre2_user_recall()
        # uur.save_pre3_user_recall()
        # uur.save_user_recall()
        # uur.save_user_tmp_recall_topK()
        # # 历史过滤用的全量的历史数据
        # uur.save_user_filter_history_recall()
        # uur.save_user_filter_version_recall()
        # uur.save_user_recall_hot_score_year_factor()
        # uur.save_user_profile_latest_recall()
        pass


    def update_similar_recall_run(self):
        """
        用户增量召回结果都保存在 增量数据库update_user中
        :return:
        """
        usr = SaveUpdateUserSimilarRecall(self.cate_id)
        """
        # 用户历史相似度召回基于方法 save_action_weight_normal() 即 action_weight_normal 数据表
        # 基于 全量的 movie_recall_1969_100 电影召回表
        """
        usr.save_user_similar_recall()
        usr.save_filter_same_recall()
        usr.save_filter_history_recall()
        usr.save_user_similar_latest_recall()
        import gc
        del usr
        gc.collect()


def train_movie_profile(full):
    up = Update()
    import time
    from datetime import datetime
    # 本地是北京时间，time.time获取时间戳时需要先+8时区
    start = int(time.time()) + 3600 * 8
    start_str = datetime.strftime(datetime.utcfromtimestamp(start), "%Y%m%d %H:%M:%S")

    # 最好一周全量更新一次，即设置full为True
    up.update_movie_profile(full=full)
    import gc
    del up
    gc.collect()

    end = int(time.time()) + 3600 * 8
    end_str = datetime.strftime(datetime.utcfromtimestamp(end), "%Y%m%d %H:%M:%S")
    spend = (end - start) / 60
    print("影视画像更新开始时间：{}".format(start_str))
    print("影视画像更新运行时长：{} 分钟".format(spend))
    print("影视画像更新结束时间：{}".format(end_str))


def train_factor():
    up = Update()
    import time
    from datetime import datetime
    # 本地是北京时间，time.time获取时间戳时需要先+8时区
    start = int(time.time()) + 3600 * 8
    start_str = datetime.strftime(datetime.utcfromtimestamp(start), "%Y%m%d %H:%M:%S")

    up.update_factor()
    import gc
    del up
    gc.collect()

    end = int(time.time()) + 3600 * 8
    end_str = datetime.strftime(datetime.utcfromtimestamp(end), "%Y%m%d %H:%M:%S")
    spend = (end - start) / 60
    print("统计权重更新开始时间：{}".format(start_str))
    print("统计权重更新运行时长：{} 分钟".format(spend))
    print("统计权重更新结束时间：{}".format(end_str))


def train_history(full=False, first=False):
    import time
    from datetime import datetime
    up = Update()

    start = int(time.time()) + 3600 * 8
    start_str = datetime.strftime(datetime.utcfromtimestamp(start), "%Y%m%d %H:%M:%S")

    """
    用户历史只有第一次计算历史数据时才用 full=True，一旦计算过一次，就会存在历史merge表，不需要再计算之前的历史
    只需计算最近时间的表，union进去就好了（针对当天重复计算，内部有用 dropDuplicates 去重）    
    """
    if first:
        # up.update_user_history(full=full)
        up.update_user_history(full=True)
    else:
        up.update_user_history(full=False)

    end = int(time.time()) + 3600 * 8
    end_str = datetime.strftime(datetime.utcfromtimestamp(end), "%Y%m%d %H:%M:%S")
    spend = (end - start) / 60
    print("用户历史更新开始时间：{}".format(start_str))
    print("用户历史更新运行时长：{} 分钟".format(spend))
    print("用户历史更新结束时间：{}".format(end_str))


def train_user_profile(full=False):
    import time
    from datetime import datetime
    up = Update()

    start = int(time.time()) + 3600 * 8
    start_str = datetime.strftime(datetime.utcfromtimestamp(start), "%Y%m%d %H:%M:%S")

    # 默认更新最近7天的用户数据
    # 此处更新的是action_weight
    up.update_user_profile_run(full=full)

    end = int(time.time()) + 3600 * 8
    end_str = datetime.strftime(datetime.utcfromtimestamp(end), "%Y%m%d %H:%M:%S")
    spend = (end - start) / 60
    print("用户画像更新开始时间：{}".format(start_str))
    print("用户画像更新运行时长：{} 分钟".format(spend))
    print("用户画像更新结束时间：{}".format(end_str))


def train_recall(channel, cate_id):
    import time
    from datetime import datetime
    up = Update(channel=channel, cate_id=cate_id)

    start = int(time.time()) + 3600 * 8
    start_str = datetime.strftime(datetime.utcfromtimestamp(start), "%Y%m%d %H:%M:%S")

    movieProfile = m_spark.sql(
        'select * from {}.update_movie_profile'.format(update_movie_db))
    flag = movieProfile.where('cate_id = {}'.format(cate_id))
    # 默认计算全量的电影召回
    up.update_movie_recall_run(flag)

    del movieProfile
    del flag
    end = int(time.time()) + 3600 * 8
    end_str = datetime.strftime(datetime.utcfromtimestamp(end), "%Y%m%d %H:%M:%S")
    spend = (end - start) / 60
    print("{} 频道，基于电影召回更新开始时间：{}".format(channel, start_str))
    print("{} 频道，基于电影召回更新运行时长：{} 分钟".format(channel, spend))
    print("{} 频道，基于电影召回更新结束时间：{}".format(channel, end_str))

    # up.update_profile_recall_run()
    up.update_similar_recall_run()
    import gc
    del up
    gc.collect()

    end2 = int(time.time()) + 3600 * 8
    end2_str = datetime.strftime(datetime.utcfromtimestamp(end2), "%Y%m%d %H:%M:%S")
    spend = (end2 - end) / 60
    print("{} 频道，基于用户召回更新开始时间：{}".format(channel, end_str))
    print("{} 频道，基于用户召回更新运行时长：{} 分钟".format(channel, spend))
    print("{} 频道，基于用户召回更新结束时间：{}".format(channel, end2_str))



if __name__ == '__main__':
    train_movie_profile(False)
    # train_factor()
    # train_history(full=True, first=True)
    # train_user_profile(False)
    # train_recall('电影', 1969)
    # train_recall('电视剧', 1970)
    # train_recall('综艺', 1971)










    # train_recall('少儿', 1973)

    # Update().update_movie_profile(full=False)
    # Update().update_user_history()
    # Update().update_user_history(update=False, cal_history=False)
    # SaveUserProfile().save_merge_action(start_time=0)
    # Update().update_user_profile_run(full=False)

    """
    脚本传参顺序：
    1. full:  (False / True)   必传
    2. profile:  (user / movie)
    3. channel:  ('电影' ..)
    4. cate_id:  (1969 ..)
    """
    # import sys
    # # 第一个参数传递是否为全量或增量计算
    # full = sys.argv[1].lower()
    # if full == 'false':
    #     full = False
    # else:
    #     full = True
    # param = sys.argv[2]
    # if param == 'movie':
    #     train_movie_profile(full)
    # elif param == 'user':
    #     train_user_profile(full)
    # else:
    #     channel = sys.argv[2]
    #     cate_id = sys.argv[3]
    #     train_recall(channel, cate_id)


