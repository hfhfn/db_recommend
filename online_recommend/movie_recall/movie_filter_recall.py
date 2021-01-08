from utils.cut_words import FilterCutWords
from utils.default import movie_recall_db, m_topK, minDCS, k, factor_db, online_db, movie_original_db
from utils.save_tohive import RetToHive


class MovieFilterRecall(object):

    def __init__(self, spark, cate_id):
        self.database_name = movie_recall_db
        self.factor_db = factor_db
        self.online_db = online_db
        self.cate_id = cate_id
        self.spark = spark
        self.topK, self.k, self.minDCS = m_topK, k, minDCS

    def get_filter_version_recall(self, update=None):
        if update:
            # print("计算更新数据")
            recall_df = update
        else:
            recall_df = self.spark.sql("select * from movie_recall.movie_recall_{}_k{}m{}"
                                       .format(self.cate_id, self.k, self.minDCS))
        # 增加一列数据， 电影title， 用来对同一个电影多版本和花絮进行过滤
        movie_df = self.spark.sql("select id, cid, title from movie.db_asset")\
                    .where('cid={}'.format(self.cate_id)).drop('cid')
        recall_df = recall_df.join(movie_df, recall_df.movie_id2 == movie_df.id,
                                   how='left').drop('id')

        # 对title分词，有可能返回空列表, 把这些title全部写入用户字典, 不知道什么原因用户字典某些词不管用，单独拎出来处理。
        recall_df = FilterCutWords(cate_id=self.cate_id, filter='movie').get_words(recall_df)
        # FilterCutWords中对分词为空的 做了处理，已不存在分词为空的
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
            Window.partitionBy('movie_id', 'word').orderBy(recall_df['title_len'].asc())))
        ret = ret.where('sort_len = 1').drop('word', 'sort_len', 'title_len')

        ret = ret.withColumn('count', fn.count("*").over(Window.partitionBy('movie_id', 'movie_id2'))) \
            .dropDuplicates(['movie_id', 'movie_id2'])

        # 如果 count < words_len 则说明这个电影因重复被删除过， 所以剔除
        ret = ret.filter('count=words_len').drop('words_len', 'count')
        # ret = ret.union(tmp_df)

        return ret  # movie_id|movie_id2|cos_sim| title|

    def get_filter_hot_score_year(self, update=None):
        import gc

        if update:
            # print("计算更新数据")
            recall_df = update.withColumnRenamed('title', 'title2')
        else:
            recall_df = self.spark.sql(
                'select *, title title2 from movie_recall.movie_filter_version_recall_{}'.format(self.cate_id)).drop(
                'title')
        title_df = self.spark.sql('select id, title, year, cid from movie.db_asset').where(
            'cid={}'.format(self.cate_id)).drop('cid')
        recall_df = recall_df.join(title_df, recall_df.movie_id == title_df.id, how='left').drop('id')

        del title_df
        hot_df = self.spark.sql(
            'select aid, cid, play_num, factor hot_factor from {}.movie_hot_factor'.format(self.factor_db)).where(
            'cid={}'.format(self.cate_id)).drop('cid')
        score_df = self.spark.sql(
            'select aid score_id, cid, score, factor score_factor from {}.movie_score_factor'.format(
                self.factor_db)).where('cid={}'.format(self.cate_id)).drop('cid')
        if self.cate_id == 1971:  # 综艺
            year_df = self.spark.sql(
                'select movie_id year_id, year year2, factor from {}.movie_year_factor_{}'.format(self.factor_db,
                                                                                                  self.cate_id))
        else:
            year_df = self.spark.sql(
                'select movie_id year_id, year year2, factor from {}.movie_year_factor'.format(self.factor_db))
        tmp_df = recall_df.join(hot_df, recall_df.movie_id2 == hot_df.aid, how='left')
        tmp_df = tmp_df.join(score_df, tmp_df.movie_id2 == score_df.score_id, how='left')
        tmp_df = tmp_df.join(year_df, tmp_df.movie_id2 == year_df.year_id, how='left').drop('aid', 'score_id',
                                                                                            'year_id').dropna()

        del hot_df
        del score_df
        del year_df
        gc.collect()

        def map(row):
            weight = row.cos_sim * row.score_factor * row.hot_factor * row.factor

            return row.movie_id, row.title, row.year, row.movie_id2, row.title2, row.year2, row.score, row.play_num, \
                   round(float(weight), 8)

        recall_df = tmp_df.rdd.map(map)

        del tmp_df
        gc.collect()

        recall_df = recall_df.toDF(
            ['movie_id', 'title', 'year', 'movie_id2', 'title2', 'year2', 'score', 'play_num', 'weight'])

        return recall_df

    def get_movie_latest_recall(self, update=False):
        import gc
        if update:
            # print("计算更新数据")
            recall_ret = update
        else:
            recall_ret = self.spark.sql("select * from movie_recall.movie_recall_factor_{}".format(self.cate_id))

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

        def se_map(row):
            title = row.title.strip().lower()
            title2 = row.title2.strip().lower()
            # 取标题前2个字符，判断相同title
            title3 = title2[:4]
            # 过滤掉名称相同和名称大于16个字的影片
            if title[:4] == title3 or len(title2) > 16:
                return None, None, None, None, None, None, None, None, None, None
            return row.movie_id, row.title, row.year, row.movie_id2, row.title2, row.year2, row.score, row.play_num, \
                   row.weight, title3

        def other_map(row):
            title = row.title.strip().lower()
            title2 = row.title2.strip().lower()
            # 取标题前2个字符，判断相同title
            title3 = title2[:4]
            if title[:4] == title3:
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
        elif self.cate_id == 1973:  # 少儿
            se_df = self.spark.sql("select id movie_id2, cate from {}.db_asset where cid = 1973".format(movie_original_db))
            # 去除不带标签的影片
            recall_ret = recall_ret.join(se_df, on='movie_id2', how='left').where('cate != ""').drop('cate')
            # 去除时长小于60分钟的 单剧集的影片
            eptotal_df = self.spark.sql("select aid movie_id2, eptotal from {}.db_asset_source \
                    where source = 12 and eptotal = 1 and get_json_object(extra, '$.len')/60 < 60".format(movie_original_db))
            recall_ret = recall_ret.join(eptotal_df, on='movie_id2', how='left').where('eptotal is null').drop('eptotal')

            del se_df
            del eptotal_df
            recall_df = recall_ret.rdd.map(se_map).toDF(
                ['movie_id', 'title', 'year', 'movie_id2', 'title2', 'year2', 'score', 'play_num', 'weight',
                 'title3']).dropna()
        else:
            recall_df = recall_ret.rdd.map(other_map).toDF(
                ['movie_id', 'title', 'year', 'movie_id2', 'title2', 'year2', 'score', 'play_num', 'weight',
                 'title3']).dropna()

        del recall_ret
        gc.collect()

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

        # 去除预告片
        movie_auth = self.spark.sql("select aid movie_id2, auth from {}.movie_auth".format(factor_db))
        recall_df = recall_df.join(movie_auth, on='movie_id2', how='left').where('auth is null').drop('auth')

        del movie_auth
        # 添加是否内部资源字段  source = 12 为内部资源，status = 1 上线
        movie_source = self.spark.sql("select aid movie_id2, source from {}.db_asset_source"
                                      .format(movie_original_db)).where('source = 12 and status = 1')
        recall_df = recall_df.join(movie_source, on='movie_id2', how='inner')

        del movie_source
        gc.collect()

        # def source_map(row):
        #     if row.source == 12:
        #         source_num = 0
        #     else:
        #         source_num = 1
        #     return  row.movie_id, row.title, row.year, row.movie_id2, row.title2, row.year2, row.score, \
        #             row.play_num, row.weight, source_num
        #
        # recall_df = recall_df.rdd.map(source_map).toDF(['movie_id', 'title', 'year', 'movie_id2', 'title2', 'year2',
        #                                                 'score', 'play_num', 'weight', 'source_num'])
        #
        # recall_df = recall_df.withColumn('sort_num', fn.row_number()
        #      .over(Window.partitionBy('movie_id').orderBy(recall_df['source_num'], recall_df['weight'].desc()))) \
        #     .withColumn('timestamp', fn.lit(day_timestamp))

        recall_df = recall_df.withColumn('sort_num', fn.row_number()
             .over(Window.partitionBy('movie_id').orderBy(recall_df['weight'].desc()))) \
            .withColumn('timestamp', fn.lit(day_timestamp))

        # print(day_timestamp)
        if update:
            # print("插入更新结果")
            try:
                recall_df.write.insertInto('{}.movie_recall_{}_{}'.format(self.database_name, self.cate_id, self.topK),
                                           overwrite=True)
            except:
                RetToHive(self.spark, recall_df, self.database_name, 'movie_recall_{}_{}'.format(self.cate_id, self.topK))

            # 单独保存一份 推荐需要的字段 用于上线的表
            online_recall = recall_df.select('movie_id', 'title', 'movie_id2', 'title2', 'sort_num', 'timestamp')
            try:
                online_recall.write.insertInto('{}.movie_recall_{}_{}'.format(self.online_db, self.cate_id, self.topK),
                                       overwrite=True)
            except:
                RetToHive(self.spark, online_recall, self.online_db, 'movie_recall_{}_{}'.format(self.cate_id, self.topK))
        return recall_df


if __name__ == '__main__':
    pass
