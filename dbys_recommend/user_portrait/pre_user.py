from utils import user_pre_db, user_original_db, movie_original_db, u_spark, factor_db


def get_user_collect(spark):
    """
    :return: 【dataframe】返回预处理好的数据表
    """
    spark.sql("use {}".format(user_original_db))
    # player: 类型 1：爱奇艺2：百事通, type: 状态（1：收藏 2取消收藏）vid: 视频id，devid：设备id
    scan_table = spark.sql('select * from tp_pagecollect').where('player = 1 and type = 1')\
        .dropDuplicates(['devid', 'vid', 'type'])

    return scan_table

def get_user_play(spark):
    """
    :return: 【dataframe】 返回提取出json字段中数据的用户行为表
    """
    def extract_play(row):
        global false, null, true
        # eval在python语言中不能识别小写的false和true还有null
        false = null = true = ''
        play_time= None
        try:
            tmp_dict = eval(row.last_play)['param']
            # print(tmp_dict)
            play_time = int(tmp_dict.get('startTime', -10))
        except:
            pass

        if row.devid == '':
            user_id = row.userid
        else:
            user_id = row.devid

        return user_id, row.aid, row.cid, play_time, row.total_num, row.eqlen, row.uptime

    totalnum_singletime = spark.sql("select aid, total_num, eqlen from {}.movie_type_factor".format(factor_db))

    user_play = spark.sql("select * from {}.tp_record_play".format(user_original_db))

    user_play = user_play.join(totalnum_singletime, on='aid', how='left').dropna()
    # print(user_play.count())  # 27815420
    # print(user_play.dropna().count())  # 25504909

    tmp_table = user_play.rdd.map(extract_play)\
                .toDF(['user_id', 'movie_id', 'cate_id', 'play_time', 'total_num', 'movie_time', 'datetime'])

    return tmp_table


if __name__ == '__main__':
    get_user_play(u_spark)
    pass
