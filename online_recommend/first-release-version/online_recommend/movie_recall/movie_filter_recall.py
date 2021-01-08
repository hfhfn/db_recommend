from movie_portrait import MovieFilterCutWords
from utils import movie_recall_db, cate_id, m_topK, k, minDCS, factor_db


class MovieFilterRecall(object):

    def __init__(self, spark):
        self.database_name = movie_recall_db
        self.factor_db = factor_db
        self.cate_id = cate_id
        self.spark = spark
        self.topK, self.k, self.minDCS = m_topK, k, minDCS

    def get_filter_version_recall(self, update=None):
        if update:
            recall_df = update
        else:
            recall_df = self.spark.sql("select * from movie_recall.movie_recall_{}_k{}m{}"
                                       .format(self.cate_id, self.k, self.minDCS))
        # 增加一列数据， 电影title， 用来对同一个电影多版本和花絮进行过滤
        movie_df = self.spark.sql("select id, title from movie.db_asset").where('cid={}'.format(self.cate_id))
        recall_df = recall_df.join(movie_df, recall_df.movie_id2 == movie_df.id,
                                   how='left').drop('id')
        # 对title分词，有可能返回空列表, 把这些title全部写入用户字典, 不知道什么原因用户字典某些词不管用，单独拎出来处理。
        recall_df = MovieFilterCutWords().get_words(recall_df)
        # 分词为空的直接留下，不做处理
        tmp_df = recall_df.where('words_len = 0').filter('title_len < 15').drop('words', 'words_len', 'title_len')
        # print(tmp_df.count())    # 22357
        import pyspark.sql.functions as fn
        # 过滤掉 title长度大于 15 的电影 （判断为花絮）
        # fn.explode在列表为空时，会删除此条数据，不会返回结果
        recall_df = recall_df.filter('title_len < 15').withColumn('word', fn.explode('words')).drop('words')

        from pyspark.sql import Window
        ret = recall_df.withColumn('sort_len', fn.row_number().over(
            Window.partitionBy('movie_id', 'word').orderBy(recall_df['title_len'].asc())))
        ret = ret.where('sort_len = 1').drop('word', 'sort_len', 'title_len')

        ret = ret.withColumn('count', fn.count("*").over(Window.partitionBy('movie_id', 'movie_id2'))) \
            .dropDuplicates(['movie_id', 'movie_id2'])

        # 如果 count < words_len 则说明这个电影因重复被删除过， 所以剔除
        ret = ret.filter('count=words_len').drop('words_len', 'count')
        ret = ret.union(tmp_df)

        return ret  # movie_id|movie_id2|cos_sim| title|

    def get_filter_hot_score_year(self, update=None):
        if update:
            recall_df = update.withColumnRenamed('title', 'title2')
        else:
            recall_df = self.spark.sql(
                'select *, title title2 from movie_recall.movie_filter_version_recall_{}'.format(self.cate_id)).drop(
                'title')
        title_df = self.spark.sql('select id, title, year, cid from movie.db_asset').where(
            'cid={}'.format(self.cate_id)).drop('cid')
        recall_df = recall_df.join(title_df, recall_df.movie_id == title_df.id, how='left').drop('id')

        hot_df = self.spark.sql(
            'select aid, cid, play_num, factor hot_factor from {}.movie_hot_factor'.format(self.factor_db)).where(
            'cid={}'.format(self.cate_id)).drop('cid')
        score_df = self.spark.sql(
            'select aid score_id, cid, score, factor score_factor from {}.movie_score_factor'.format(
                self.factor_db)).where('cid={}'.format(self.cate_id)).drop('cid')
        year_df = self.spark.sql(
            'select movie_id year_id, year year2, factor from {}.movie_year_factor'.format(self.factor_db))
        tmp_df = recall_df.join(hot_df, recall_df.movie_id2 == hot_df.aid, how='left')
        tmp_df = tmp_df.join(score_df, tmp_df.movie_id2 == score_df.score_id, how='left')
        tmp_df = tmp_df.join(year_df, tmp_df.movie_id2 == year_df.year_id, how='left').drop('aid', 'score_id',
                                                                                            'year_id').dropna()

        def map(row):
            weight = row.cos_sim * row.score_factor * row.hot_factor * row.factor

            return row.movie_id, row.title, row.year, row.movie_id2, row.title2, row.year2, row.score, row.play_num, \
                   round(float(weight), 8)

        recall_df = tmp_df.rdd.map(map).toDF(
            ['movie_id', 'title', 'year', 'movie_id2', 'title2', 'year2', 'score', 'play_num', 'weight'])

        return recall_df

    def get_movie_latest_recall(self, update=False):
        if update:
            recall_ret = update
        else:
            recall_ret = self.spark.sql("select * from movie_recall.movie_recall_factor_{}".format(self.cate_id))

        def map(row):
            title = row.title2.lower()
            # 取标题前2个字符，判断相同title
            title3 = title[:2]
            # 取标题后2个字符，判断异常电影 =》 奥斯卡金像奖， 观影指南， 上影节， 奥斯卡...
            title4 = title[-2:]
            title5 = title[:1] + title[3:4]
            if title4 in ['像奖', '指南', '影节', '斯卡', '直击', '鲜看'] or title5 in ['【】', '第届']:
                return None, None, None, None, None, None, None, None, None, None
            return row.movie_id, row.title, row.year, row.movie_id2, row.title2, row.year2, row.score, row.play_num, \
                   row.weight, title3

        recall_df = recall_ret.rdd.map(map).toDF(
            ['movie_id', 'title', 'year', 'movie_id2', 'title2', 'year2', 'score', 'play_num', 'weight',
             'title3']).dropna()
        import pyspark.sql.functions as fn
        from pyspark.sql import Window
        # 对version_filter分词去重没有成功的，再次利用title切片去重
        recall_df = recall_df.withColumn('sort_num', fn.row_number().over(
            Window.partitionBy('movie_id', 'title3').orderBy(recall_df['weight'].desc()))) \
            .where('sort_num = 1').drop('title3', 'sort_num')

        import time
        import datetime
        today = datetime.date.today()
        day_timestamp = int(today.strftime("%Y%m%d"))
        # day_timestamp = int(time.mktime(time.strptime(str(today), '%Y-%m-%d')))  # 零点时间戳

        recall_df = recall_df.withColumn('sort_num', fn.row_number()
                                         .over(Window.partitionBy('movie_id').orderBy(recall_df['weight'].desc()))) \
            .withColumn('timestamp', fn.lit(day_timestamp))
        # print(day_timestamp)
        if update:
            # print("插入电影召回数据")
            recall_df.write.insertInto('{}.movie_recall_{}_{}'.format(self.database_name, self.cate_id, self.topK),
                                       overwrite=True)
        return recall_df


if __name__ == '__main__':
    pass
