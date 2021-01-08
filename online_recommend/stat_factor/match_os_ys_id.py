from utils.default import factor_db
from utils.spark_app import MovieDataApp


spark = MovieDataApp().spark

def get_match_id():
    # movie_play有的aid对应多个qid，以aid去重
    ys_df = spark.sql("select qid, aid ys_id, cid from {}.movie_play".format(factor_db)).dropDuplicates(['ys_id'])
    # db_asset_source这个表aid有重复的，去重
    os_df = spark.sql("select aid os_id, source, title, sid from movie.db_asset_source")\
                            .where('source=12').dropDuplicates(['os_id'])
    # 查看过没有重复的
    os_aid_cid = spark.sql("select id, cid os_cid from movie.db_asset")
    os_df = os_df.join(os_aid_cid, os_df.os_id == os_aid_cid.id, how='left').drop('id')
    # os_df.show()
    tmp = os_df.join(ys_df, (os_df.sid == ys_df.qid) & (os_df.os_cid ==ys_df.cid), how='left')\
                .dropDuplicates(['os_id']).drop('source', 'sid', 'qid', 'cid').dropna()

    return tmp     # os_id, title, os_cid, ys_id