from utils import user_pre_db, user_original_db


def get_pre_table(spark, table_type, index):
    """
    :param table_type: 【str】'click' or 'top' or 'play'
    :param index: 【int】此前每种（点击，收藏，播放）数据一共有22张表，代表传入第几张表
    :return: 【dataframe】返回预处理好的数据表
    """
    import re
    spark.sql("use {}".format(user_original_db))
    click_table = []
    top_table = []
    play_table = []
    table_name = spark.sql("show tables")
    # table_name.show(truncate=False)
    table_name = table_name.rdd.map(lambda row: row.tableName)
    num = table_name.count()
    table_list = table_name.take(num)
    for i in table_list:
        # print(i)
        try:
            tag = re.findall(u".*_([a-z]+)\d+", i)[0]
            # print(table_type)
            if tag == 'enter':
                click_table.append(i)
            elif tag == 'top':
                top_table.append(i)
            else:
                play_table.append(i)
        except:
            continue
    if table_type == "click":
        click_table.sort(reverse=False)
        table_list = click_table
    elif table_type == 'play':
        play_table.sort(reverse=False)
        table_list = play_table
    else:
        top_table.sort(reverse=False)
        table_list = top_table
        scan_table = spark.sql(
            'select mac, funitems, ymd from {} where funtype = "focus"'.format(table_list[index])).where(
            'mac <> ""')
        return scan_table
    scan_table = spark.sql('select mac, funitems, ymd from {}'.format(table_list[index])).where('mac <> ""')
    # scan_table.show(5, truncate=False)
    return scan_table

def pre_user_data(table_type, pre_table):
    """
    :param table_type: 【str】'click' or 'top' or 'play'
    :param pre_table: 【dataframe】 预处理好的原数据
    :return: 【dataframe】 返回提取出json字段中数据的用户行为表
    """
    import gc
    def extract_play(row):
        global false, null, true
        # eval在python语言中不能识别小写的false和true还有null
        false = null = true = ''
        aid, cid, play_time, movie_num = None, None, None, None
        try:
            tmp_dict = eval(row.funitems)
            # print(tmp_dict)
            aid = int(tmp_dict.get('aid'))
            cid = int(tmp_dict.get('cid'))
            # play_time = int(tmp_dict.get('video_time_point', '-10'))   此字段不可用
            play_time = int(tmp_dict.get('play_duration', '-10'))  # -10 代表无此值 相当于None
            movie_num = int(tmp_dict.get('ep_num', '-10'))
            # aid, cid, play_time, movie_num = tmp_dict['aid'], \
            #                       tmp_dict['cid'], tmp_dict['video_time_point'], tmp_dict['ep_num']
            # print(aid, cid, play_time, play_time2, movie_num)
        except:
            pass
        return row.mac, aid, cid, play_time, movie_num, row.ymd

    def extract_click_top(row):
        global false, null, true
        # eval在python语言中不能识别小写的false和true还有null
        false = null = true = ''
        aid, cid = None, None
        try:
            tmp_dict = eval(row.funitems)
            aid, cid = int(tmp_dict.get('aid')), int(tmp_dict.get('cid'))
        except:
            pass
        return row.mac, aid, cid, row.ymd

    if table_type == "play":
        from pyspark.shell import sqlContext
        # tmp_table = pre_table.rdd.map(extract_play)\
        #     .toDF(['user_id', 'movie_id', 'cate_id', 'play_time', 'movie_num', 'datetime'])
        tmp_table = pre_table.rdd.map(extract_play)
        tmp_table = sqlContext.createDataFrame(tmp_table,
                                               ['user_id', 'movie_id', 'cate_id', 'play_time', 'movie_num',
                                                'datetime'],
                                               samplingRatio=0.2).dropna()
        tmp_table = tmp_table.where('play_time != 0 and play_time != -10')
    else:
        tmp_table = pre_table.rdd.map(extract_click_top) \
            .toDF(['user_id', 'movie_id', 'cate_id', 'datetime']).dropna()

    gc.collect()
    return tmp_table


def merge_user_data(spark, table_type, table_num, start_num=0):
    """
    :param table_type: 【str】 'click' or 'top' or 'play'
    :return: 【dataframe】 返回每种行为数据所有表的merge表
    """
    spark.sql("use {}".format(user_pre_db))
    if start_num == 0:
        ret = None
    else:
        # 本来想运行完，删除掉改过名的表merge_{}_{}，但在保存前删除造成依赖此表的结果丢失，所以应再保存完后下一步再删除
        spark.sql('ALTER TABLE merge_{} RENAME TO merge_{}_{}'.format(table_type, table_type, start_num))
        ret = spark.sql('select * from merge_{}_{}'.format(table_type, start_num))
        # 也可以复制创建一个表实现下面功能
        # spark.sql('create TABLE merge_{}_{} SELECT * FROM merge_{}'.format(table_type, start_num, table_type))
        # 零时表行不通，覆盖 merge_{}表时，零时表也会随之清零
        # ret = spark.sql('select * from merge_{}'.format(table_type))
        # ret.registerTempTable("temptable")
        # ret = spark.sql('select * from temptable')

    k = start_num
    if table_type == 'play':
        for i in range(start_num, table_num):
            try:
                pre_table = spark.sql('select * from user_{}_{}'.format(table_type, i))
                pre_table = pre_table.rdd.map(lambda row: (row.user_id, row.movie_id, row.cate_id,
                                                           int(row.play_time / (1000 * 60)), row.movie_num,
                                                           row.datetime)) \
                    .toDF(['user_id', 'movie_id', 'cate_id', 'play_time', 'movie_num', 'datetime'])
                # merge_ret = merge_ret.withColumn('play_time2', merge_ret.play_time / (1000 * 60))\
                #     .drop('play_time').withColumnRenamed("play_time2", "play_time")
                pre_table = pre_table.where('play_time > 1 and play_time < 200')
                if k == 0:
                    ret = pre_table
                    k += 1
                else:
                    ret = ret.union(pre_table)
            except:
                continue
    else:
        for i in range(start_num, table_num):
            try:
                pre_table = spark.sql('select * from user_{}_{}'.format(table_type, i))
                if k == 0:
                    ret = pre_table
                    k += 1
                else:
                    ret = ret.union(pre_table)
            except:
                continue
    # 对结果去重，同一个人同一天对同一个电影的相同行为只算一次，一方面去除同一天多次相同行为，
    # 另一方面因为10天一张表，每天merge表只有一天的新行为，其他10天内的行为会被重复合并，需要去重
    ret.dropDuplicates()
    return ret


if __name__ == '__main__':
    pass
