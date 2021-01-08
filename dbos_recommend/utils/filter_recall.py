from utils import factor_db, FilterCutWords, RetToHive, user_recall_db, movie_original_db, movie_recall_db, \
        pre_topK, k, minDCS, m_topK, online_db, als_db


class FilterRecall(object):
    factor_db = factor_db
    # spark = FilterApp().spark
    movie_recall_db = movie_recall_db
    pre_topK = pre_topK
    k, minDCS = k, minDCS

    def __init__(self, spark, cate_id, recall_db):
        self.spark = spark
        self.cate_id = cate_id
        self.recall_db = recall_db


    def get_filter_history_recall(self, update=None, recall='vector'):
        """
        :param recall:   vector, action, profile
        :return:
        """
        self.spark.sql('use {}'.format(self.factor_db))
        # 只过滤 用户播放和点击历史记录
        user_history_play = self.spark.sql('select user_id uid, movie_id mid from user_history_play') \
            .where('cate_id = {}'.format(self.cate_id))
        user_history_click = self.spark.sql('select user_id uid, movie_id mid from user_history_click') \
            .where('cate_id = {}'.format(self.cate_id))
        user_history = user_history_play.union(user_history_click).dropDuplicates(['uid', 'mid'])

        if update:
            user_recall = update
        else:
            if recall == 'vector':
                # vector_recall
                user_recall = self.spark.sql('select * from {}.user_similar_{}'.format(self.recall_db, self.cate_id))
            elif recall == 'action':
                # action_recall
                user_recall = self.spark.sql(
                    'select * from {}.user_similar_filter_same_recall_{}'.format(self.recall_db, self.cate_id))
            else:
                # profile_recall
                user_recall = self.spark.sql(
                    'select * from {}.user_recall_{}_{}'.format(self.recall_db, self.cate_id, self.pre_topK))

        ret = user_recall.join(user_history,
                               (user_recall.user_id == user_history.uid) & (user_recall.movie_id == user_history.mid),
                               how='left').where('mid is null').drop('uid', 'mid')
        """
        def del_history(row):
            if row.mid is not None:
                return None, None, None, None, None, None, None, None
            else:
                return row.user_id, row.movie_id, row.title, row.year, row.score, row.play_num, row.weight, \
                       str(row.base_movie)

        ret = ret.rdd.map(del_history).toDF(
            ['user_id', 'movie_id', 'title', 'year', 'score', 'play_num', 'weight', 'base_movie']).dropna()
        """
        import time
        import datetime
        today = datetime.date.today()
        day_timestamp = int(today.strftime('%Y%m%d'))
        # day_timestamp = int(time.mktime(time.strptime(str(today), '%Y-%m-%d')))  # 零点时间戳

        import pyspark.sql.functions as fn
        from pyspark.sql import Window
        ret = ret.withColumn("sort_num", fn.row_number().over(
            Window.partitionBy("user_id").orderBy(ret["weight"].desc()))).withColumn('timestamp', fn.lit(day_timestamp))

        # ret.agg(*[(1 - (fn.count(c) / fn.count('*'))).alias(c + '_missing') for c in ret.columns]).show()
        return ret


    def get_filter_version_recall(self, update=None, filter='user'):
        """
        :param update:
        :param recall:  user, movie
        :return:
        """
        if update:
            # print("计算更新数据")
            recall_df = update
        else:
            # profile_recall
            if filter == 'user':
                # 此方法要对title分词并进行explode，数据量比较大， 建议先选topK再进行版本过滤
                recall_df = self.spark.sql(
                    "select * from {}.user_filter_history_recall_{}".format(self.recall_db, self.cate_id))

            # action_recall
            else:
                recall_df = self.spark.sql("select * from {}.movie_recall_{}_k{}m{}"
                                   .format(self.movie_recall_db, self.cate_id, self.k, self.minDCS))

        # 增加一列数据， 电影title， 用来对同一个电影多版本和花絮进行过滤
        movie_df = self.spark.sql("select id, cid, title from {}.db_asset".format(movie_original_db))\
                            .where('cid={}'.format(self.cate_id)).drop('cid')
        if filter == 'user':
            # 1302709 此id电影 被分类到 0 频道， 造成join时 title为null， 需要dropna处理一下，不然后续分词过滤会出错
            recall_df = recall_df.join(movie_df, recall_df.movie_id == movie_df.id, how='left').drop('id').dropna()
            # recall_df.where('title is null').show(100)
        else:
            recall_df = recall_df.join(movie_df, recall_df.movie_id2 == movie_df.id, how='left').drop('id').dropna()
        # recall_df.show()
        # 对title分词，有可能返回空列表, 把这些title全部写入用户字典, 由于分词词频的原因用户字典某些词不被使用，单独拎出来处理。
        recall_df = FilterCutWords(cate_id=self.cate_id, filter='{}'.format(filter)).get_words(recall_df)
        # FilterCutWords中对分词为空的 做了处理，已不存在分词为空的，不需要再考虑
        """
        # 分词为空的直接留下，不做处理
        # tmp_df = recall_df.where('words_len = 0').filter('title_len < 15').drop('words', 'words_len', 'title_len')
        # tmp_df = recall_df.where('words_len = 0').drop('words', 'words_len', 'title_len')
        # print(tmp_df.count())    # 22357
        """
        import pyspark.sql.functions as fn
        # 过滤掉 title长度大于 15 的电影 （判断为花絮）
        # fn.explode在列表为空时，会删除此条数据，不会返回结果
        if self.cate_id in [1971, 1973]:    # 综艺或者少儿，不对标题长度进行过滤
            recall_df = recall_df.withColumn('word', fn.explode('words')).drop('words')
        else:
            recall_df = recall_df.filter('title_len < 15').withColumn('word', fn.explode('words')).drop('words')

        from pyspark.sql import Window
        ret = recall_df.withColumn('sort_len', fn.row_number().over(
            Window.partitionBy('{}_id'.format(filter), 'word').orderBy(recall_df['title_len'].asc())))
        # 去重电影title相似的，留下名字最短的一个 （权宜之计，名字最短的是正片的可能更大）
        ret = ret.where('sort_len = 1').drop('word', 'sort_len', 'title_len')
        """
        # tmp = ret.groupby('user_id', 'movie_id').count()\
        #             .withColumnRenamed('user_id', 'uid').withColumnRenamed('movie_id', 'mid')
        # 00:9E:C8:D6:E7:B1| 1007715  以上代码统计count为null， 很多类似这样的统计为null的， 不知什么原因
        # ret = ret.dropDuplicates(['user_id', 'movie_id'])
        # print(tmp.count(), ret.count())
        # ret = ret.join(tmp, (ret.user_id==tmp.uid) & (ret.movie_id==tmp.mid), how='left').drop('uid', 'mid')
        # ret = ret.where('count is null').show(100)
        """
        if filter == 'user':
            ret = ret.withColumn('count', fn.count("*").over(Window.partitionBy('user_id', 'movie_id'))) \
                .dropDuplicates(['user_id', 'movie_id'])
        else:
            ret = ret.withColumn('count', fn.count("*").over(Window.partitionBy('movie_id', 'movie_id2'))) \
                .dropDuplicates(['movie_id', 'movie_id2'])

        # 如果 count < words_len 则说明这个电影因重复被删除过， 所以剔除
        ret = ret.filter('count=words_len').drop('words_len', 'count')
        # ret = ret.union(tmp_df)

        """
        # from pyspark.shell import sqlContext
        # schema = StructType([StructField('user_id', StringType(), True), StructField('movie_id', IntegerType(), True),
        #                     StructField('cate_id', IntegerType(), True), StructField('weight', DoubleType(), True),
        #                     StructField('title', StringType(), True)])
        # ret = sqlContext.createDataFrame(ret, schema=schema).dropna()
        # ret = sqlContext.createDataFrame(ret, ['user_id', 'movie_id', 'cate_id', 'weight', 'title'],
        #                                        samplingRatio=0.4)#.dropna()
        """
        # 报错了， 一般可以保存结束了单独查看
        # ret.agg(*[(1 - (fn.count(c) / fn.count('*'))).alias(c + '_missing') for c in ret.columns]).show()

        return ret  # movie_id|movie_id2|cos_sim| title|


    def get_filter_hot_score_year(self, update=None, filter='movie'):
        if update:
            # print("计算更新数据")
            recall_df = update
            if filter == 'movie':
                recall_df = recall_df.withColumnRenamed('title', 'title2')
            title_df = self.spark.sql('select id, title, year, cid from {}.db_asset'.format(movie_original_db))\
                .where('cid={}'.format(self.cate_id)).drop('cid')
            recall_df = recall_df.join(title_df, recall_df.movie_id == title_df.id, how='left').drop('id')
            if filter == 'user':
                recall_df = recall_df.withColumnRenamed('movie_id', 'movie_id2')
        else:
            if filter == 'movie':
                recall_df = self.spark.sql(
                    'select *, title title2 from {}.movie_filter_version_recall_{}'.format(
                        self.movie_recall_db, self.cate_id)).drop('title')
                title_df = self.spark.sql('select id, title, year, cid from {}.db_asset'.format(movie_original_db))\
                    .where('cid={}'.format(self.cate_id)).drop('cid')
                recall_df = recall_df.join(title_df, recall_df.movie_id == title_df.id, how='left').drop('id')
            else:
                # 为了 以下方法中的join 共用一个 id 去关联， 把所有的 关联id 都零时改为 movie_id2
                recall_df = self.spark.sql(
                    "select *, movie_id movie_id2 from {}.user_filter_version_recall_{}".format(
                        self.recall_db, self.cate_id)).drop('movie_id')

        hot_df = self.spark.sql(
            'select aid movie_id2, cid, play_num, factor hot_factor from {}.movie_hot_factor'.format(
                self.factor_db)).where('cid={}'.format(self.cate_id)).drop('cid')
        score_df = self.spark.sql(
            'select aid movie_id2, cid, score, factor score_factor from {}.movie_score_factor'.format(
                self.factor_db)).where('cid={}'.format(self.cate_id)).drop('cid')
        if self.cate_id == 1971:  # 综艺
            year_df = self.spark.sql(
                'select movie_id movie_id2, year year2, factor from {}.movie_year_factor_{}'.format(
                    self.factor_db, self.cate_id))
        else:
            year_df = self.spark.sql(
                'select movie_id movie_id2, year year2, factor from {}.movie_year_factor'.format(self.factor_db))

        tmp_df = recall_df.join(hot_df, on='movie_id2', how='left')
        tmp_df = tmp_df.join(score_df, on='movie_id2', how='left')
        tmp_df = tmp_df.join(year_df, on='movie_id2', how='left')

        def movie_map(row):
            weight = row.cos_sim * row.score_factor * row.hot_factor * row.factor

            return row.movie_id, row.title, row.year, row.movie_id2, row.title2, row.year2, row.score, row.play_num, \
                   round(float(weight), 8)

        def user_map(row):
            try:
                weight = row.cos_sim * row.hot_factor * row.score_factor * row.factor
            except:
                weight = row.weight * row.hot_factor * row.score_factor * row.factor
            return row.user_id, row.movie_id2, row.title, row.year2, row.play_num, row.score, \
                   round(float(weight), 8)

        if filter == 'movie':
            recall_df = tmp_df.rdd.map(movie_map).toDF(
                ['movie_id', 'title', 'year', 'movie_id2', 'title2', 'year2', 'score', 'play_num', 'weight'])
        else:
            recall_df = tmp_df.rdd.map(user_map).toDF(
                ['user_id', 'movie_id', 'title', 'year', 'play_num', 'score', 'weight'])

        import pyspark.sql.functions as fn
        from pyspark.sql import Window
        recall_df = recall_df.withColumn('sort_num', fn.row_number().over(
            Window.partitionBy('{}_id'.format(filter)).orderBy(recall_df['weight'].desc()))).where('sort_num <= 100')

        return recall_df


class LatestFilterRecall(object):

    def __init__(self, spark, cate_id, recall_db):
        self.movie_recall_db = movie_recall_db
        self.user_recall_db = recall_db
        self.factor_db = factor_db
        self.online_db = online_db
        self.cate_id = cate_id
        self.spark = spark
        self.topK, self.k, self.minDCS = m_topK, k, minDCS

    def get_movie_latest_recall(self, update=False):
        if update:
            # print("计算更新数据")
            recall_ret = update
        else:
            recall_ret = self.spark.sql("select * from {}.movie_recall_factor_{}".format(
                                                                    self.movie_recall_db, self.cate_id))

        movie_filter = ['像奖', '指南', '影节', '斯卡', '直击', '鲜看', '剧集', '音乐', '纪念', '电影', '体验']

        def movie_map(row):
            title = row.title.strip().lower()
            title2 = row.title2.strip().lower()
            # 取标题前2个字符，判断相同title
            title3 = title2[:2]
            # 取标题后2个字符，判断异常电影 =》 奥斯卡金像奖， 观影指南， 上影节， 奥斯卡...
            title4 = title2[-2:]
            title5 = title2[:1] + title2[3:4]
            if title4 in movie_filter or title5 in ['【】', '第届'] or '《' in list(title2) or title[:2] == title3:
                return None, None, None, None, None, None, None, None, None, None
            return row.movie_id, row.title, row.year, row.movie_id2, row.title2, row.year2, row.score, row.play_num, \
                   row.weight, title3

        """
        首映礼,片场手记,明星专访,速看版,卫视春晚,经典老歌,合集,精编版,定制版,精华版,饭制合集,KK战队,幕后纪实,全程回顾,
        短视频,高能剧场,星光大赏,vlog,戏精日常,干货版,主题曲演唱,CP版,乒乓社日常,小剧场,纪录片,品质盛典,官方MV,番外篇
        荣誉时刻，
        """
        tv_filter = ['映礼', '手记', '专访', '看版', '春晚', '老歌', '合集', '编版', '制版', '华版', '战队', '纪实',
                     '回顾', '剧场', '大赏', 'og', '日常', '货版', '演唱', 'p版', '录片', '盛典', '速看',
                     'mv', '外篇', '视频', '唱会']

        def tv_map(row):
            title = row.title.strip().lower()
            title2 = row.title2.strip().lower()
            # 取标题前2个字符，判断相同title
            title3 = title2[:2]
            # 取标题后2个字符，判断异常电影 =》 奥斯卡金像奖， 观影指南， 上影节， 奥斯卡...
            title4 = title2[-2:]
            if title4 in tv_filter or '《' in list(title2) or title[:2] == title3:
                return None, None, None, None, None, None, None, None, None, None
            return row.movie_id, row.title, row.year, row.movie_id2, row.title2, row.year2, row.score, row.play_num, \
                   row.weight, title3

        def other_map(row):
            title = row.title.strip().lower()
            title2 = row.title2.strip().lower()
            # 取标题前2个字符，判断相同title
            title3 = title2[:2]
            if title[:2] == title3:
                return None, None, None, None, None, None, None, None, None, None
            return row.movie_id, row.title, row.year, row.movie_id2, row.title2, row.year2, row.score, row.play_num, \
                   row.weight, title3

        if self.cate_id == 1969:
            recall_df = recall_ret.rdd.map(movie_map).toDF(
                ['movie_id', 'title', 'year', 'movie_id2', 'title2', 'year2', 'score', 'play_num', 'weight',
                 'title3']).dropna()
        elif self.cate_id == 1970:  # 电视剧
            recall_df = recall_ret.rdd.map(tv_map).toDF(
                ['movie_id', 'title', 'year', 'movie_id2', 'title2', 'year2', 'score', 'play_num', 'weight',
                 'title3']).dropna()
        else:
            recall_df = recall_ret.rdd.map(other_map).toDF(
                ['movie_id', 'title', 'year', 'movie_id2', 'title2', 'year2', 'score', 'play_num', 'weight',
                 'title3']).dropna()

        import gc
        del recall_ret
        gc.collect()

        import time
        import datetime
        today = datetime.date.today()
        day_timestamp = int(today.strftime('%Y%m%d'))
        # day_timestamp = int(time.mktime(time.strptime(str(today), '%Y-%m-%d')))  # 零点时间戳

        import pyspark.sql.functions as fn
        from pyspark.sql import Window
        # 对version_filter分词去重没有成功的，再次利用title切片去重
        recall_df = recall_df.withColumn('sort_num', fn.row_number().over(
            Window.partitionBy('movie_id', 'title3').orderBy(recall_df['weight'].desc()))) \
            .where('sort_num = 1').drop('title3', 'sort_num')

        movie_source = self.spark.sql("select aid movie_id2, source from {}.db_asset_source".format(movie_original_db))\
                    .where('source = 12')
        recall_df = recall_df.join(movie_source, on='movie_id2', how='left')

        def source_map(row):
            if row.source == 12:
                source_num = 0
            else:
                source_num = 1
            return  row.movie_id, row.title, row.year, row.movie_id2, row.title2, row.year2, row.score, \
                    row.play_num, row.weight, source_num

        recall_df = recall_df.rdd.map(source_map).toDF(
            ['movie_id', 'title', 'year', 'movie_id2', 'title2', 'year2', 'score', 'play_num', 'weight', 'source_num'])

        # source字段 为12 的是内部资源
        recall_df = recall_df.withColumn('sort_num', fn.row_number()
             .over(Window.partitionBy('movie_id').orderBy(recall_df['source_num'], recall_df['weight'].desc()))) \
            .withColumn('timestamp', fn.lit(day_timestamp))

        # print(day_timestamp)
        if update:
            # print("插入更新结果")
            RetToHive(self.spark, recall_df, self.movie_recall_db, 'movie_recall_{}_{}'.format(self.cate_id, self.topK))
            # recall_df.write.insertInto('{}.movie_recall_{}_{}'.format(self.database_name, self.cate_id, self.topK),
            #                            overwrite=True)
            online_recall = recall_df.select('movie_id', 'title', 'movie_id2', 'title2', 'sort_num', 'timestamp')
            online_recall.write.insertInto('{}.movie_recall_{}_{}'.format(self.online_db, self.cate_id, self.topK),
                                       overwrite=True)
            # RetToHive(self.spark, online_recall, self.online_db, 'movie_recall_{}_{}'.format(self.cate_id, self.topK))
        return recall_df


    def get_user_latest_recall(self, update=False, recall='vector'):
        if update:
            # print("计算更新数据")
            recall_ret = update
        else:
            recall_ret = self.spark.sql("select * from {}.movie_recall_factor_{}".format(self.user_recall_db, self.cate_id))

        movie_filter = ['像奖', '指南', '影节', '斯卡', '直击', '鲜看', '剧集', '音乐', '纪念', '电影', '体验', '看版']

        def movie_map(row):
            title = row.title.strip().lower()
            # 取标题前2个字符，判断相同title
            title3 = title[:2]
            # 取标题后2个字符，判断异常电影 =》 奥斯卡金像奖， 观影指南， 上影节， 奥斯卡...
            title4 = title[-2:]
            title5 = title[:1] + title[3:4]
            if title4 in movie_filter or title5 in ['【】', '第届'] or '《' in list(title):
                return None, None, None, None, None, None, None, None
            return row.user_id, row.movie_id, row.title, row.year, row.score, row.play_num, \
                   row.weight, title3

        """
        首映礼,片场手记,明星专访,速看版,卫视春晚,经典老歌,合集,精编版,定制版,精华版,饭制合集,KK战队,幕后纪实,全程回顾,
        短视频,高能剧场,星光大赏,vlog,戏精日常,干货版,主题曲演唱,CP版,乒乓社日常,小剧场,纪录片,品质盛典,官方MV,番外篇
        荣誉时刻，
        """
        tv_filter = ['映礼', '手记', '专访', '看版', '春晚', '老歌', '合集', '编版', '制版', '华版', '战队', '纪实',
                     '回顾', '剧场', '大赏', 'og', '日常', '货版', '演唱', 'p版', '录片', '盛典', '速看',
                     'mv', '外篇', '视频', '唱会']

        def tv_map(row):
            title = row.title.strip().lower()
            # 取标题前2个字符，判断相同title
            title3 = title[:2]
            # 取标题后2个字符，判断异常电影 =》 奥斯卡金像奖， 观影指南， 上影节， 奥斯卡...
            title4 = title[-2:]
            if title4 in tv_filter or '《' in list(title):
                return None, None, None, None, None, None, None, None
            return row.user_id, row.movie_id, row.title, row.year, row.score, row.play_num, \
                   row.weight, title3

        def other_map(row):
            title = row.title.strip().lower()
            # 取标题前2个字符，判断相同title
            title3 = title[:2]
            return row.user_id, row.movie_id, row.title, row.year, row.score, row.play_num, \
                   row.weight, title3

        if self.cate_id == 1969:
            recall_df = recall_ret.rdd.map(movie_map).toDF(
                ['user_id', 'movie_id', 'title', 'year', 'score', 'play_num', 'weight', 'title3']).dropna()
        elif self.cate_id == 1970:  # 电视剧
            recall_df = recall_ret.rdd.map(tv_map).toDF(
                ['user_id', 'movie_id', 'title', 'year', 'score', 'play_num', 'weight', 'title3']).dropna()
        else:
            recall_df = recall_ret.rdd.map(other_map).toDF(
                ['user_id', 'movie_id', 'title', 'year', 'score', 'play_num', 'weight', 'title3']).dropna()

        import gc
        del recall_ret
        gc.collect()

        import time
        import datetime
        today = datetime.date.today()
        day_timestamp = int(today.strftime('%Y%m%d'))
        # day_timestamp = int(time.mktime(time.strptime(str(today), '%Y-%m-%d')))  # 零点时间戳

        import pyspark.sql.functions as fn
        from pyspark.sql import Window
        # 对version_filter分词去重没有成功的，再次利用title切片去重
        recall_df = recall_df.withColumn('sort_num', fn.row_number().over(
            Window.partitionBy('user_id', 'title3').orderBy(recall_df['weight'].desc()))) \
            .where('sort_num = 1').drop('title3', 'sort_num')

        movie_source = self.spark.sql("select aid movie_id, source from {}.db_asset_source".format(movie_original_db))\
                    .where('source = 12')
        recall_df = recall_df.join(movie_source, on='movie_id', how='left')

        def source_map(row):
            if row.source == 12:
                source_num = 0
            else:
                source_num = 1
            return  row.user_id, row.movie_id, row.title, row.year, row.score, \
                    row.play_num, row.weight, source_num

        recall_df = recall_df.rdd.map(source_map).toDF(
            ['user_id', 'movie_id', 'title', 'year', 'score', 'play_num', 'weight', 'source_num'])

        # source字段 为12 的是内部资源
        recall_df = recall_df.withColumn('sort_num', fn.row_number()
             .over(Window.partitionBy('user_id').orderBy(recall_df['source_num'], recall_df['weight'].desc()))) \
            .withColumn('timestamp', fn.lit(day_timestamp))

        # print(day_timestamp)
        if update:
            if recall == 'vector':
                # print("插入更新结果")
                RetToHive(self.spark, recall_df, self.user_recall_db, 'vector_recall_{}_{}'.format(self.cate_id, self.topK))
                # recall_df.write.insertInto('{}.movie_recall_{}_{}'.format(self.database_name, self.cate_id, self.topK),
                #                            overwrite=True)
                online_recall = recall_df.select('user_id', 'movie_id', 'title', 'sort_num', 'timestamp')
                # online_recall.write.insertInto('{}.vector_recall_{}_{}'.format(self.online_db, self.cate_id, self.topK),
                #                            overwrite=True)
                RetToHive(self.spark, online_recall, self.online_db, 'vector_recall_{}_{}'.format(self.cate_id, self.topK))
            elif recall == 'als':
                # print("插入更新结果")
                RetToHive(self.spark, recall_df, self.user_recall_db, 'als_recall_{}_{}'.format(self.cate_id, self.topK))
                # recall_df.write.insertInto('{}.movie_recall_{}_{}'.format(self.database_name, self.cate_id, self.topK),
                #                            overwrite=True)
                online_recall = recall_df.select('user_id', 'movie_id', 'title', 'sort_num', 'timestamp')
                # online_recall.write.insertInto('{}.vector_recall_{}_{}'.format(self.online_db, self.cate_id, self.topK),
                #                            overwrite=True)
                RetToHive(self.spark, online_recall, self.online_db, 'als_recall_{}_{}'.format(self.cate_id, self.topK))

        return recall_df



if __name__ == '__main__':
    pass
