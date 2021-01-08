from movie_portrait import UserFilterCutWords
from utils import cate_id, pre_topK, u_topK, movie_original_db, user_recall_db, factor_db


# from pyspark.sql.types import *


class UserFilterRecall(object):

    def __init__(self, pre_db, recall_db, spark):
        self.spark = spark
        self.cate_id = cate_id
        self.pre_topK = pre_topK
        self.topK = u_topK
        self.user_pre_db = pre_db
        self.user_recall_db = recall_db
        self.user_history_db = factor_db

    def get_filter_history_recall(self):
        # self.spark.sql('use {}'.format(self.user_recall_db))
        self.spark.sql('use {}'.format(self.user_history_db))
        # import time
        # from datetime import timedelta
        # interval = timedelta(days=60)
        # timestamp = int(time.time()) - int(interval.total_seconds())
        # 只过滤 用户播放和点击历史记录
        user_history_play = self.spark.sql('select user_id uid, movie_id mid from user_history_play') \
            .where('cate_id = {}'.format(self.cate_id))
        user_history_click = self.spark.sql('select user_id uid, movie_id mid from user_history_click') \
            .where('cate_id = {}'.format(self.cate_id))
        user_history = user_history_play.union(user_history_click).dropDuplicates(['uid', 'mid'])

        user_recall = self.spark.sql('select * from user_recall_{}_{}'.format(self.cate_id, self.pre_topK))
        ret = user_recall.join(user_history, (user_recall.user_id == user_history.uid) &
                               (user_recall.movie_id == user_history.mid), how='left')

        # ret = ret.dropDuplicates(['user_id', 'movie_id'])

        def del_history(row):
            if row.mid is not None:
                return None, None, None, None
            else:
                return row.user_id, row.movie_id, row.cate_id, row.weight

        ret = ret.rdd.map(del_history).toDF(['user_id', 'movie_id', 'cate_id', 'weight']).dropna()

        # 以下代码废弃
        # import pyspark.sql.functions as fn
        # from pyspark.sql import Window
        # ret = ret.dropna().withColumn("sort_num",
        #                          fn.row_number().over(Window.partitionBy("user_id").orderBy(ret["weight"].desc())))
        # df = spark.sql('select * from user_mac')
        # ret = ret.join(df, ret.user_id == df.deviceid, how='left').drop('user_id').drop('deviceid')\
        #                     .withColumnRenamed('mac', 'user_id')

        # ret.agg(*[(1 - (fn.count(c) / fn.count('*'))).alias(c + '_missing') for c in ret.columns]).show()
        return ret

    def get_filter_version_recall(self):
        """
        此方法要对title分词并进行explode，数据量比较大， 建议先选topK再进行版本过滤
        :return:
        """
        recall_df = self.spark.sql("select * from {}.user_filter_history_recall_{}".format(self.user_recall_db, self.cate_id))
        # 增加一列数据， 电影title， 用来对同一个电影多版本和花絮进行过滤
        movie_df = self.spark.sql("select id, cid, title from {}.db_asset".format(movie_original_db))
        recall_df = recall_df.join(movie_df, (recall_df.cate_id == movie_df.cid) & (recall_df.movie_id == movie_df.id),
                                   how='left').drop('id', 'cid')
        # 对title分词，有可能返回空列表
        recall_df = UserFilterCutWords().get_words(recall_df).filter('title_len < 15')
        # 分词为空的直接留下，不做处理
        tmp_df = recall_df.where('words_len = 0').drop('words', 'words_len', 'title_len')
        # print(tmp_df.count())    # 227825   去除“《”后 227825

        import pyspark.sql.functions as fn
        # fn.explode在列表为空时，会删除此条数据，不会返回结果
        recall_df = recall_df.withColumn('word', fn.explode('words')).drop('words')
        # print(recall_df.dropDuplicates(['user_id', 'movie_id']).count())
        from pyspark.sql import Window
        ret = recall_df.withColumn('sort_len', fn.row_number().over(
            Window.partitionBy('user_id', 'word').orderBy(recall_df['title_len'].asc())))
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
        ret = ret.withColumn('count', fn.count("*").over(Window.partitionBy('user_id', 'movie_id'))) \
            .dropDuplicates(['user_id', 'movie_id'])

        # ret = spark.sql('select * from user_recall.tmp_user_filter_version_recall_{}'.format(cate_id))

        # 如果 count < words_len 则说明这个电影因重复被删除过， 所以剔除
        ret = ret.filter('count=words_len').drop('words_len', 'count')
        ret = ret.union(tmp_df)
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
        return ret

    def get_action_stat(self):
        """
        统计用户行为数据
        :return:
        """
        action = self.spark.sql('select user_id, cate_id, datetime, click, top, play_time from {}.merge_action'.format(self.user_pre_db))
        # .where('cate_id = {}'.format(cate_id)).drop('cate_id')
        click_count = action.where('click = 1').groupby('user_id', 'cate_id').count() \
            .withColumnRenamed('count', 'click_count').withColumnRenamed('user_id', 'click_id') \
            .withColumnRenamed('cate_id', 'click_cate')
        # click_count.show()
        top_count = action.where('top = 1').groupby('user_id', 'cate_id').count().withColumnRenamed('count',
                                                                                                    'top_count') \
            .withColumnRenamed('user_id', 'top_id').withColumnRenamed('cate_id', 'top_cate')
        play_count = action.where('play_time > 0').groupby('user_id', 'cate_id').count() \
            .withColumnRenamed('count', 'play_count').withColumnRenamed('user_id', 'play_id') \
            .withColumnRenamed('cate_id', 'play_cate')
        tmp = action.select('user_id', 'cate_id').dropDuplicates(['user_id', 'cate_id'])

        tmp = tmp.join(click_count, (tmp.user_id == click_count.click_id) & (tmp.cate_id == click_count.click_cate),
                       how='left').drop('click_id', 'click_cate')
        # tmp.show()
        tmp = tmp.join(top_count, (tmp.user_id == top_count.top_id) & (tmp.cate_id == top_count.top_cate), how='left') \
            .drop('top_id', 'top_cate')
        tmp = tmp.join(play_count, (tmp.user_id == play_count.play_id) & (tmp.cate_id == play_count.play_cate),
                       how='left') \
            .drop('play_id', 'play_cate')

        click_latest = action.where('click = 1').groupby('user_id', 'cate_id').agg({'datetime': 'max'}) \
            .withColumnRenamed('max(datetime)', 'click_latest').withColumnRenamed('user_id', 'click_id') \
            .withColumnRenamed('cate_id', 'click_cate')
        top_latest = action.where('top = 1').groupby('user_id', 'cate_id').agg({'datetime': 'max'}) \
            .withColumnRenamed('max(datetime)', 'top_latest').withColumnRenamed('user_id', 'top_id') \
            .withColumnRenamed('cate_id', 'top_cate')
        play_latest = action.where('play_time > 0').groupby('user_id', 'cate_id').agg({'datetime': 'max'}) \
            .withColumnRenamed('max(datetime)', 'play_latest').withColumnRenamed('user_id', 'play_id') \
            .withColumnRenamed('cate_id', 'play_cate')

        tmp = tmp.join(click_latest, (tmp.user_id == click_latest.click_id) & (tmp.cate_id == click_latest.click_cate),
                       how='left').drop('click_id', 'click_cate')
        tmp = tmp.join(top_latest, (tmp.user_id == top_latest.top_id) & (tmp.cate_id == top_latest.top_cate),
                       how='left') \
            .drop('top_id', 'top_cate')
        tmp = tmp.join(play_latest, (tmp.user_id == play_latest.play_id) & (tmp.cate_id == play_latest.play_cate),
                       how='left').drop('play_id', 'play_cate')

        def stat_action(row):
            click_count = row.click_count
            top_count = row.top_count
            play_count = row.play_count
            try:
                total_count = click_count + top_count + play_count
            except:
                if click_count is None:
                    click_count = 0
                if top_count is None:
                    top_count = 0
                if play_count is None:
                    play_count = 0
                total_count = click_count + top_count + play_count

            click_latest = row.click_latest
            top_latest = row.top_latest
            play_latest = row.play_latest
            import pytz
            from datetime import datetime
            tz = pytz.timezone('Asia/Shanghai')
            if click_latest is None:
                click_latest = ''
            else:
                t = datetime.fromtimestamp(click_latest, tz)
                click_latest = t.strftime('%Y-%m-%d')
            if top_latest is None:
                top_latest = ''
            else:
                t = datetime.fromtimestamp(top_latest, tz)
                top_latest = t.strftime('%Y-%m-%d')
            if play_latest is None:
                play_latest = ''
            else:
                t = datetime.fromtimestamp(play_latest, tz)
                play_latest = t.strftime('%Y-%m-%d')

            return row.user_id, row.cate_id, click_count, click_latest, top_count, top_latest, \
                   play_count, play_latest, total_count

        ret = tmp.rdd.map(stat_action).toDF(['user_id', 'cate_id', 'click_count', 'click_latest',
                                             'top_count', 'top_latest', 'play_count', 'play_latest', 'total_count'])
        return ret

    def get_user_latest_recall(self):
        recall_ret = self.spark.sql(
            "select * from {}.user_recall_factor_{}".format(self.user_recall_db, self.cate_id)) \
            .where('sort_num <= {}'.format(self.topK))
        def map(row):
            title = row.title.lower()
            # 取标题前2个字符，判断相同title
            title2 = title[:2]
            # 判断异常电影 =》 奥斯卡金像奖， 观影指南， 上影节， 奥斯卡, 1月新片抢鲜看,
            # 第69届戛纳电影节全程直击, 【公益】泰斯特-只看TA2， 龙虎少年队3D短片3
            # 86届奥斯卡获奖作品展, 剧情类最佳女主角,
            # 其它：《007：大破天幕杀机》拍摄日志，007电影经典片头大赏，0202测试，10亿票房大片·4K免费看
            # 10大震撼心灵的电影瞬间，10大最美古装女神，10月观影指南，10亿票房俱乐部，11月全球热映电影混剪
            title3 = title[-2:]
            title4 = title[:1] + title[3:4]
            if title3 in ['像奖', '指南', '影节', '斯卡', '直击', '鲜看'] or title4 in ['【】', '第届']:
                return None, None, None, None, None, None, None, None
            return row.user_id, row.movie_id, row.title, row.year, row.score, row.play_num, row.weight, title2

        recall_df = recall_ret.rdd.map(map).toDF(
            ['user_id', 'movie_id', 'title', 'year', 'score', 'play_num', 'weight', 'title2']).dropna()
        import pyspark.sql.functions as fn
        from pyspark.sql import Window
        # 对version_filter分词去重没有成功的，再次利用title切片去重
        recall_df = recall_df.withColumn('sort_num', fn.row_number().over(
            Window.partitionBy('user_id', 'title2').orderBy(recall_df['weight'].desc()))) \
            .where('sort_num = 1').drop('title2', 'sort_num')

        import time
        import datetime
        today = datetime.date.today()
        day_timestamp = int(time.mktime(time.strptime(str(today), '%Y-%m-%d')))  # 零点时间戳

        recall_df = recall_df.withColumn('sort_num', fn.row_number()
                                         .over(Window.partitionBy('user_id').orderBy(recall_df['weight'].desc()))) \
            .withColumn('timestamp', fn.lit(day_timestamp))

        return recall_df


if __name__ == '__main__':
    pass

# def get_user_mac(table_type):
#     import gc
#     spark.sql('use userdata')
#     table_name = spark.sql("show tables")
#     # table_name.show(truncate=False)
#     table_name = table_name.rdd.map(lambda row: row.tableName)
#     num = table_name.count()
#     table_list = table_name.take(num)
#     click_table = []
#     top_table = []
#     play_table = []
#     import re
#     for i in table_list:
#         try:
#             tag = re.findall(u".*_([a-z]+)\d+", i)[0]
#             if tag == 'enter':
#                 click_table.append(i)
#             elif tag == 'top':
#                 top_table.append(i)
#             else:
#                 play_table.append(i)
#         except:
#             continue
#     if table_type == "click":
#         table_list = click_table
#     elif table_type == 'play':
#         table_list = play_table
#     else:
#         table_list = top_table
#     for i, table in enumerate(table_list):
#         tmp_table = spark.sql('select deviceid, mac from {}'.format(table)).filter('mac <> ""')\
#                     .dropDuplicates(['deviceid', 'mac']).dropna()
#         if i == 0:
#             df = tmp_table
#         else:
#             df = df.union(tmp_table)
#
#     df = df.dropDuplicates(['deviceid', 'mac'])
#     gc.collect()
#     return df
#
# def merge_user_mac():
#     spark.sql('use user_recall')
#     click = spark.sql('select * from user_mac_click')
#     top = spark.sql('select * from user_mac_top')
#     play = spark.sql('select * from user_mac_play')
#     df = click.union(top).union(play).dropDuplicates(['deviceid', 'mac'])
#
#     return df
