from stat_factor.movie_auth import get_movie_auth
from stat_factor.movie_hot import get_movie_hot_sort, get_movie_hot_factor
from stat_factor.movie_score import get_movie_score_factor
from stat_factor.movie_time import get_movie_time
from stat_factor.movie_total_num import get_movie_total_num
from stat_factor.movie_year import get_movie_year_factor
from stat_factor.user_history import get_user_history
from utils.default import factor_db
from utils.spark_app import MovieDataApp
from utils.save_tohive import RetToHive


database_name = factor_db
spark_app = MovieDataApp().spark

# def save_match_id():
#     # 保存当贝影视和当贝os 对应id结果    62159
#     predata_ret = get_match_id()
#     predata_table = 'match_id'
#     RetToHive(spark_app, predata_ret, database_name, predata_table)
#
#
# def save_stat_play():
#     # 保存当贝影视播放数据    23246852
#     predata_ret = get_stat_play()
#     predata_table = 'movie_play'
#     RetToHive(spark_app, predata_ret, database_name, predata_table)


def save_movie_hot_sort():
    # 把当贝影视近3个月热播数据join到当贝OS中作为热播，并保存统计数据和排序   310983
    movie_hot_ret = get_movie_hot_sort()
    movie_hot_table = 'movie_hot_sort'
    RetToHive(spark_app, movie_hot_ret, database_name, movie_hot_table)
    import gc
    del movie_hot_ret
    gc.collect()


def save_movie_hot_factor():
    # 把热播数据进行对数衰减处理，作为加权因子  310983
    movie_hot_ret = get_movie_hot_factor()
    movie_hot_table = 'movie_hot_factor'
    RetToHive(spark_app, movie_hot_ret, database_name, movie_hot_table)
    import gc
    del movie_hot_ret
    gc.collect()


def save_movie_score_factor():
    # 保存当贝OS 评分加权的数据   310983
    movie_score_ret = get_movie_score_factor()
    movie_score_table = 'movie_score_factor'
    RetToHive(spark_app, movie_score_ret, database_name, movie_score_table)
    import gc
    del movie_score_ret
    gc.collect()


def save_movie_time():
    # 保存movie_time结果    226254
    movie_time_ret = get_movie_time()
    movie_time_table = 'movie_time'
    RetToHive(spark_app, movie_time_ret, database_name, movie_time_table)
    import gc
    del movie_time_ret
    gc.collect()


def save_movie_year_factor(cate_id):
    # 保存电影年代衰减因子    300794
    year_factor_ret = get_movie_year_factor(cate_id=cate_id)
    if cate_id == 1971:  # 综艺
        year_factor_table = 'movie_year_factor_{}'.format(cate_id)
    else:
        year_factor_table = 'movie_year_factor'
    RetToHive(spark_app, year_factor_ret, database_name, year_factor_table)
    import gc
    del year_factor_ret
    gc.collect()

def save_user_history():
    # 保存用户历史行为    click: 3391634, top: 88240, play: 1120225
    for history_type in ['click', 'top', 'play']:
        user_history_ret = get_user_history(history_type)
        user_history_table = 'user_history_{}'.format(history_type)
        RetToHive(spark_app, user_history_ret, database_name, user_history_table)
    import gc
    del user_history_ret
    gc.collect()


def save_user_history_cate(cate_id):
    """
    不参与模型其他计算， 只是用来测试查看对比历史信息用
    :return:
    """
    # 保存电影分类的用户历史行为    click: 1341437, top: 23504, play: 418929
    for history_type in ['click', 'top', 'play']:
        user_history_ret = spark_app.sql(
            'select user_id, movie_id, cate_id, datetime, title, year, sort_time from {}.user_history_{}'.format(
                database_name, history_type)).filter('cate_id={}'.format(cate_id))
        user_history_table = 'user_history_{}_{}'.format(history_type, cate_id)
        RetToHive(spark_app, user_history_ret, database_name, user_history_table)
    import gc
    del user_history_ret
    gc.collect()


def save_movie_auth():
    """
    保存预告片的电影     30476
    :return:
    """
    movie_auth_ret = get_movie_auth()
    movie_auth_table = 'movie_auth'
    RetToHive(spark_app, movie_auth_ret, database_name, movie_auth_table)
    import gc
    del movie_auth_ret
    gc.collect()


def save_movie_total_num():
    """
    保存影片总集数
    :return:
    """
    movie_total_num_ret = get_movie_total_num()
    movie_total_num_table = 'movie_total_num'
    RetToHive(spark_app, movie_total_num_ret, database_name, movie_total_num_table)
    import gc
    del movie_total_num_ret
    gc.collect()



if __name__ == '__main__':
    # save_match_id()
    # save_stat_play()
    # save_movie_hot_sort()
    # save_movie_hot_factor()
    # save_movie_score_factor()
    # save_user_history()
    save_movie_auth()
    pass
