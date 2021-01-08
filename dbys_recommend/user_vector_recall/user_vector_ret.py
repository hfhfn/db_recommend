from utils import user_portrait_db, user_recall_db, RetToHive, Word2Vector, update_user_db


def get_user_vector(spark, channel, cate_id, refit=True, update=True):
    if update:
        profile = spark.sql("select * from {}.user_profile".format(update_user_db))
    else:
        profile = spark.sql("select * from {}.user_profile".format(user_portrait_db))
    wv = Word2Vector(spark, refit, channel=channel)
    user_vector_ret = wv.get_user_vector(profile)
    user_vector_table = 'user_vector_{}'.format(cate_id)
    RetToHive(spark, user_vector_ret, user_recall_db, user_vector_table)
    import gc
    del user_vector_ret
    del wv
    gc.collect()