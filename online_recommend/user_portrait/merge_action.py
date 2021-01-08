from utils.spark_app import UserDataApp
# from utils import channelInfo


def add_action(table, user_pre_db):
    import re
    import pyspark.sql.functions as F
    spark = UserDataApp().spark
    spark.sql("use {}".format(user_pre_db))
    action = re.findall(u".*_([a-z]+)", table)[0]
    merge_tmp = spark.sql('select * from {}'.format(table))
    # F.lit() 增加一列固定值
    merge_tmp = merge_tmp.withColumn('action', F.lit(action))

    return merge_tmp


def _compute(row):
    _list = []

    class Temp(object):
        click = False
        top = False
        # 原始数据中play_time没有的话就是 -10，此处统一用 -10 代表None含义
        play_time = -10
        total_num = -10

    _tp = Temp()
    # 进行判断行为类型
    if row.action == "click":
        _tp.click = True
    elif row.action == "top":
        _tp.top = True
    elif row.action == "play":
        _tp.play_time = row.play_time
        _tp.total_num = row.total_num
    else:
        pass
    _list.append(
        [row.user_id, row.movie_id, row.cate_id, row.datetime, _tp.click, _tp.top, _tp.play_time, _tp.total_num])
    return _list

# 进行处理
def get_merge_action(user_pre_db, start_time=0):
    tmp_data = None
    tmp_li = ['merge_click', 'merge_top', 'merge_play']
    for i, table in enumerate(tmp_li):
        tmpDF = add_action(table, user_pre_db)
        _res = tmpDF.rdd.flatMap(_compute)
        data = _res.toDF(
            ["user_id", "movie_id", "cate_id", "datetime", "click", "top", "play_time", "total_num"])
        # data.show()
        if i == 0:
            tmp_data = data
        else:
            tmp_data = tmp_data.union(data)
            # print(tmp_data.count())
            # tmp_data = tmp_data.join(data, on=["user_id", "movie_id", 'cate_id', "datetime"], how='left')
    # print(tmp_data.count())
    # merge后的数据有些行为数据中可能有null
    return tmp_data.where('datetime > {}'.format(start_time)).dropna()



if __name__ == '__main__':

    # a = add_action('merge_click').where('movie_id = 1121176 or movie_id = 965149').limit(10)
    # b = add_action('merge_top').where('movie_id = 1121176 or movie_id = 965149').limit(10)
    # c = a.join(b, on=["user_id", "movie_id", "datetime", 'cate_id'], how='outer')
    # d = a.join(b, on=["user_id", "movie_id", "datetime", 'cate_id'], how='inner')
    # c.show()
    # d.show()
    pass