from user_profile_recall import save_inverted_table
from movie_portrait import save_predata, save_cut_words, get_cv_idf_model
from movie_recall import SaveMovieRecall
from stat_factor import save_movie_hot_sort, save_movie_hot_factor, save_movie_score_factor, save_movie_year_factor, \
    save_user_history, save_movie_time
from update_scheduler import SaveUpdateMovieRecall, SaveUpdateUserProfileRecall, SaveUpdateUserSimilarRecall, \
    SaveUpdateUserProfile, UpdateMovie
from user_portrait import SaveUserProfile
from utils import update_user_db, user_pre_db, m_spark, movie_portrait_db, movie_recall_db, interval, \
    RetToHive, update_movie_db, u_spark, user_recall_db, get_latest_recall
from user_vector_recall import get_user_vector, compute_user_similar


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

    def update_user_history(self, update=True, cal_history=True):
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
        start_year, start_moon, start_day = 2019, 6, 10
        # 默认情况， 增量导入数据起始和结束都是同一个时间，即当前最新时间数据
        table_year, table_moon, table_day = date.split(',')  # 用来表征增量数据起始导入日期
        end_year, end_moon, end_day = date.split(',')  # 用来表征 最近数据日期 即增量数据 截止日期
        table_num = (int(end_year) - int(start_year)) * 12 * 3 + (int(end_moon) - int(start_moon)) * 3 + (
                int(end_day) - int(start_day)) // 10 + 1
        if update:
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
            print('用户历史更新完成')
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
        movie_profile_table = 'update_movie_profile'
        if sentence_df.rdd.count():
            rank, idf = self.ua.generate_movie_label(sentence_df, full=full)
            movieProfile = self.ua.get_movie_profile(rank, idf, full=full)

            # 保存更新的电影画像数据, 此处保存的数据只用来后续作为判断是否有更新数据
            movieProfile.write.insertInto('{}.{}'.format(update_movie_db, movie_profile_table), overwrite=True)
            # RetToHive(self.spark, movieProfile, update_movie_db, movie_profile_table)
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

    def update_user_similar(self):
        """
        使用 faiss 模型 直接计算 用户向量和电影向量的相似度
        :return:
        """
        user_similar = compute_user_similar(self.spark, self.cate_id)

        return user_similar

    def update_movie_recall(self):
        """
        对相似度召回结果进行过滤
        :return:
        """
        # 读取全量的 movie_similar 数据进行过滤
        movie_similar = self.spark.sql(
            'select * from {}.movie_similar_{}'.format(movie_recall_db, self.cate_id))
        movie_recall = get_latest_recall(movie_similar, self.cate_id, movie_recall_db, filter='movie')

        # 全量覆盖插入保存，返回值未被调用
        return movie_recall

    def update_vector_recall(self, recall_db, filter):
        """
        对相似度召回结果进行过滤
        :return:
        """
        # 读取 user_similar 数据进行过滤
        user_similar = self.spark.sql(
            'select * from {}.user_similar_{}'.format(user_recall_db, self.cate_id))
        user_recall = get_latest_recall(user_similar, self.cate_id, recall_db, filter)

        # 全量覆盖插入保存，返回值未被调用
        return user_recall

    def update_movie_recall_run(self, flag):
        sign = flag.rdd.count()
        if sign:  # 如果是False 说明movie没有更新数据， 不需要重新计算以下步骤
            # # 得到基于全量数据的 movie_vector，用于 movie_similar 计算
            self.update_movie_vector(refit=True)  # 基于单频道计算
            # 余弦相似度召回
            self.update_movie_similar()  # 基于单频道计算
            # 可选择一周更新，或日更，提出去所有频道只计算一次，否则在此处要多次计算
            # self.update_factor()
            # 得到最终的topK个召回集
            self.update_movie_recall()
        print('{}召回更新完成'.format(self.channel))

    def update_user_profile_run(self, full=False, recall='vector'):
        """
        用户增量数据计算结果都保存在增量数据库 update_user 中
        :recall:  profile, action, vector
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
        # 只参与乘法计算可以不用归一化
        # uup.save_action_weight_normal()
        """
        # 以上方法 在进行 历史相似度召回和 用户画像召回时 都需要计算
        # 以下方法 只在 用户画像召回时计算
        """
        if recall != 'action':
            uup.save_action_topic_weight()  # 基于 topic_weights
            uup.save_action_topic_sort()
            uup.save_user_profile()
        print('用户画像更新完成')
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

    def update_action_recall_run(self):
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
        print('用户历史相似度召回更新完成')
        import gc
        del usr
        gc.collect()

    def update_vector_recall_run(self, flag):
        sign = flag.rdd.count()
        if sign:  # 如果是False 说明movie没有更新数据， 不需要重新计算以下步骤
            # # 得到基于全量数据的 movie_vector，用于 movie_similar 计算
            self.update_movie_vector(refit=True)  # 基于单频道计算
        get_user_vector(self.spark, self.channel, self.cate_id, refit=False, update=True)  # 基于单频道计算
        # 余弦相似度召回
        self.update_user_similar()  # 基于单频道计算
        # 基于 update_factor 过滤， 得到最终的topK个召回集
        self.update_vector_recall(recall_db=update_user_db, filter='user')
        print('{}召回更新完成'.format(self.channel))


def train_movie_profile(full):
    import time
    up = Update()

    start = time.time()
    # 最好一周全量更新一次，即设置full为True
    up.update_movie_profile(full=full)
    end = time.time()
    spend = (end - start) / 60
    print("update_movie_profile 运行时长：{} 分钟".format(spend))

    start = time.time()
    up.update_factor()
    end = time.time()
    spend = (end - start) / 60
    print("update_factor 运行时长：{} 分钟".format(spend))


def train_user_profile(full):
    import time
    up = Update()

    start = time.time()
    up.update_user_history()
    end = time.time()
    spend = (end - start) / 60
    print("update_user_history 运行时长：{} 分钟".format(spend))

    start = time.time()
    # 默认更新最近7天的用户数据
    up.update_user_profile_run(full=False, recall='action')
    end = time.time()
    spend = (end - start) / 60
    print("update_user_profile 运行时长：{} 分钟".format(spend))


def train_action_recall(channel, cate_id):
    import time
    up = Update(channel=channel, cate_id=cate_id)
    start = time.time()
    movieProfile = m_spark.sql(
        'select * from {}.update_movie_profile'.format(update_movie_db))
    flag = movieProfile.where('cate_id = {}'.format(cate_id))
    # 默认计算全量的电影召回
    up.update_movie_recall_run(flag)
    end = time.time()
    spend = (end - start) / 60
    print("update_movie_recall 运行时长：{} 分钟".format(spend))

    start = time.time()
    # up.update_profile_recall_run()
    up.update_action_recall_run()
    end = time.time()
    spend = (end - start) / 60
    print("update_action_recall 运行时长：{} 分钟".format(spend))

    import gc
    del movieProfile
    del flag
    del up
    gc.collect()


def train_vector_recall(channel, cate_id):
    import time
    up = Update(channel=channel, cate_id=cate_id)
    start = time.time()
    movieProfile = m_spark.sql(
        'select * from {}.update_movie_profile'.format(update_movie_db))
    flag = movieProfile.where('cate_id = {}'.format(cate_id))
    up.update_vector_recall_run(flag)
    end = time.time()
    spend = (end - start) / 60
    print("update_vector_recall 运行时长：{} 分钟".format(spend))


if __name__ == '__main__':
    # train_profile(False)
    # train_action_recall('电影', 1969)
    # train_recall('电视剧', 1970)
    # Update().update_movie_profile(full=False)
    # Update().update_user_history(update=False, cal_history=False)
    # SaveUserProfile().save_merge_action(start_time=0)
    Update().update_user_profile_run(full=True, recall='vector')
    # train_vector_recall('电影', 1969)
    # Update().update_user_similar()
    # Update().update_vector_recall(update_user_db, filter='user')

    pass
