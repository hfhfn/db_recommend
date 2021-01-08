from utils.default import movie_portrait_db, factor_db
from utils.spark_app import UserDataApp


spark = UserDataApp().spark

def compute_weights(rowpartition):
    """
    计算每个用户对每个电影的权重
    处理每个用户对电影的行为数据
    """
    weightsOfaction = {
        "play_min": 1,  # 播放 1 ~ 6 分钟
        "play_middle": 2,  # 播放小于30% 或 小于30分钟
        "play_max": 3,  # 播放小于60% 或 小于60分钟
        "play_finish": 4,  # 播放大于60% 或 大于60分钟
        "click": 2,
        "top": 4,
    }

    import happybase
    from datetime import datetime
    import numpy as np
    #  用于读取hbase缓存结果配置
    # pool = happybase.ConnectionPool(size=10, host='hadoop-master', port=9090)

    # 读取电影的标签数据
    # 计算权重值
    # 时间间隔
    for row in rowpartition:
        dateArray = datetime.fromtimestamp(row.datetime)
        # t = datetime.now() - datetime.strptime(str(row.datetime), '%Y-%m-%d %H:%M:%S')
        t = datetime.now() - datetime.strptime(dateArray.strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")
        # 时间衰减系数
        time_exp = 1 / (np.log(t.days + 1) + 1)
        is_play = 0
        # 观看时间分数
        if row.play_time != -10:
            if row.play_time <= 6:
                is_play = weightsOfaction['play_min']
            elif row.play_time <= row.movie_len:
                p_t = row.play_time / row.movie_len
                if p_t < 0.3:
                    is_play = weightsOfaction['play_middle']
                elif p_t < 0.6:
                    is_play = weightsOfaction['play_max']
                else:
                    is_play = weightsOfaction['play_finish']
            else:
                if row.play_time < 30:
                    is_play = weightsOfaction['play_middle']
                elif row.play_time < 60:
                    is_play = weightsOfaction['play_max']
                else:
                    is_play = weightsOfaction['play_finish']

            # 权重除以剧集，（统一剧集影视的单集权重）
            try:
                is_play = is_play / row.total_num
            except:
                pass
        # 每条行为的权重分数
        weights = time_exp * (
                row.click * weightsOfaction['click'] + row.top * weightsOfaction['top'] + is_play)

        yield row.user_id, row.movie_id, row.cate_id, round(float(weights), 4)


#        with pool.connection() as conn:
#            table = conn.table('user_profile')
#            table.put('user:{}'.format(row.user_id).encode(),
#                      {'partial:{}:{}'.format(row.channel_id, row.topic).encode(): json.dumps(
#                          weigths).encode()})
#            conn.close()


def get_action_weight(user_pre_db):
    merge_action = spark.sql("select * from {}.merge_action".format(user_pre_db))
    movie_time = spark.sql("select * from {}.movie_time".format(factor_db))
    # join movit_time, 计算播放行为权重用
    merge_action_mt = merge_action.join(movie_time, on='movie_id', how='left')
    merge_action_mt = merge_action_mt.fillna(-10, ['movie_len'])
    action_weights = merge_action_mt.rdd.mapPartitions(compute_weights)\
                .toDF(['user_id', 'movie_id', 'cate_id', 'weight'])
    # print(action_weights.where('movie_id is null').count())
    import pyspark.sql.functions as fn
    action_weights = action_weights.groupby('user_id', 'movie_id')\
                .agg(fn.sum('weight').alias('weight'), fn.max('cate_id').alias('cate_id'))
    # print(action_weights.where('movie_id is null').count())
    return action_weights


def normal_action_weight(user_portrait_db):
    action_weight = spark.sql("select * from {}.action_weight".format(user_portrait_db))
    import pyspark.sql.functions as fn
    tmp = action_weight.agg(fn.max('weight').alias('max'),fn.min('weight').alias('min')).first()
    # 权重数据不适合做标准化， 因为标准化需要数据接近高斯分布（正态分布），而且权重为负数无法使用。
    # tmp = action_weight.agg(fn.mean('weight').alias('mean')).first().mean
    # print(type(tmp.mean))
    # stddev_pop 总体标准差， 如果是抽取一部分样本则使用样本标准差 stddev或者stddev_samp
    # tmp = action_weight.agg(fn.mean('weight').alias('mean'), fn.stddev_pop('weight').alias('stddev_pop')).take(1)
    # mean = tmp.mean
    # std = tmp.stddev_pop
    # def standard(row):
    #     weight = (row.weight - mean) / std

    # 归一化
    min = tmp.min
    print(min)
    max = tmp.max
    print(max)
    def normal(row):
        weight = (row.weight - min) / (max - min)
        return row.user_id, row.movie_id, row.cate_id, round(float(weight), 8)

    df = action_weight.rdd.map(normal).toDF(['user_id', 'movie_id', 'cate_id', 'weight'])
    return df


def get_action_topic_weight(user_portrait_db):
    # from utils import movie_protrait_database, user_protrait_database
    # topic_weight = spark.sql("select movie_id, topic, weight topic_weight from {}.topic_weights"
    #                          .format(movie_protrait_database))
    # action_weight = spark.sql("select user_id, movie_id, cate_id, weight action_weight from {}.action_weight"
    #                           .format(user_protrait_database))
    topic_weight = spark.sql("select movie_id, topic, weight topic_weight from {}.topic_weights_normal"
                             .format(movie_portrait_db))
    action_weight = spark.sql("select user_id, movie_id, cate_id, weight action_weight from {}.action_weight_normal"
                              .format(user_portrait_db))

    # 使用left join 发现用户行为中存在电影中没有的movie_id （因为topic和weight字段有null）， 所以使用inner join
    action_topic_weight = action_weight.join(topic_weight, on='movie_id', how='inner') \
                .withColumn('weight', action_weight.action_weight * topic_weight.topic_weight)\
                .drop('topic_weight').drop('action_weight')
    # import pyspark.sql.functions as fn
    # # 统计每一列缺失值（null）的占比
    # action_topic_weight\
    #     .agg(*[(1 - (fn.count(c) / fn.count('*'))).alias(c + '_missing') for c in action_topic_weight.columns]).show()

    return action_topic_weight


def get_action_topic_sort(user_portrait_db):
    # action_topic_weight = get_action_topic_weight()
    action_topic_weight = spark.sql("select * from {}.action_topic_weight".format(user_portrait_db))
    # action_topic_weight = action_weights.groupby('user_id', 'cate_id', 'topic').agg({'weight': 'sum'})
    from pyspark.sql import Window
    import pyspark.sql.functions as fn
    # 统计每个用户在每个类型中对应的topic词的权重
    action_topic_weight = action_topic_weight.groupby('user_id', 'cate_id', 'topic')\
                        .agg(fn.sum('weight').alias('weight'))
    # 统计每个用户在每个分类中的所有topic权重和
    action_user_weight = action_topic_weight.groupby('user_id', 'cate_id').agg(fn.sum('weight').alias('user_weight')
                   ).withColumnRenamed('user_id', 'uid').withColumnRenamed('cate_id', 'cid')
    action_topic_weight = action_topic_weight.join(action_user_weight,
        (action_topic_weight.user_id==action_user_weight.uid) & (action_topic_weight.cate_id==action_user_weight.cid),
                                                   how='left').drop('uid', 'cid')
    # 统计每个分类中用户数量
    action_user_count = action_topic_weight.groupby('cate_id').agg(fn.countDistinct('user_id').alias('user_count'))
    action_topic_weight = action_topic_weight.join(action_user_count, on='cate_id', how='left')
    # 统计每个分类中每个topic词出现在多少个不同的用户中
    action_topic_count = action_topic_weight.groupby('cate_id', 'topic').agg(fn.count('user_id').alias('topic_count'))\
                .withColumnRenamed('cate_id', 'cid').withColumnRenamed('topic', 'topic2')
    action_topic_weight = action_topic_weight.join(action_topic_count,
       (action_topic_weight.cate_id==action_topic_count.cid) & (action_topic_weight.topic==action_topic_count.topic2),
                                                   how='left').drop('cid', 'topic2')
    # action_topic_weight.printSchema()
    # 对topic词的权重基于用户画像作"tfidf"，降低同时出现在不同用户中的相同画像词的权重，以免多用户根据画像召回相同的电影
    import numpy as np
    def map(row):
        # 之前使用 count而不是topic_count来作column名字，造成 使用row.count时识别为count方法，造成错误function不能与int相加
        # user_weight 因为归一化的问题，导致最小的权重为0，除数需要加1
        # topic_count 可能在某个cate_id中没有出现过，次数为0，所以除数加1；同时分子也加1保证log自变量大于等于1，即log值大于等于0
        # 为了防止log值为0，导致相乘使得weight值为0， 需要对log值加1，确保相乘前权重在log为0时等于自身
        weight = (row.weight / (row.user_weight + 1)) * (np.log((row.user_count + 1) / (row.topic_count + 1)) + 1)
        return row.user_id, row.cate_id, row.topic, round(float(weight), 8)
    action_topic_weight = action_topic_weight.rdd.map(map).toDF(['user_id', 'cate_id', 'topic', 'weight'])
    action_topic_sort = action_topic_weight.withColumn("row_number",
                fn.row_number().over(Window.partitionBy("user_id", "cate_id").orderBy(fn.desc('weight'))))

    # action_topic_sort.show()
    # print(action_topic_sort.count())

    return action_topic_sort


def get_user_profile(user_portrait_db, topK):
    action_topic_sort = spark.sql("select * from {}.action_topic_sort".format(user_portrait_db))
    _user_profile = action_topic_sort.where('row_number <= {}'.format(topK))

    return _user_profile


if __name__ == '__main__':
    # get_action_topic_weight().show()
    get_action_topic_sort()
    # get_action_weight()
    # normal_action_weight()
    # get_user_profile(20).show(100)
