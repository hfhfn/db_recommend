from utils import m_spark, movie_original_db


def get_movie_type():
    """
    电影数据类型
    :return:
    """
    # prevue: 正片/预告片， vip: 付费/免费， fee: 影片资费，0免费，1会员免费，2单点，3用券，-1未知,
    # isCoupon:是否为用券影片，onoff: (1 on,2 自动下线,3 人工下线)， eptotal：总集数， epupdnum：已更新集数，
    # epupdnm: 更新期/集名， eqlen: 影片时长默认0，type: 资源，12 爱奇艺内部资源

    movie_type = m_spark.sql(
        'select aid, vip, fee, isCoupon, eptotal, epupdnum, eqlen from {}.tp_pagetype'
            .format(movie_original_db)).where('type = 12 and prevue = 0 and onoff = 1') # 内部资源，正片，上线
    def map(row):
        epupdnum = int(row.epupdnum)
        if epupdnum > 0 and epupdnum < 10000:
            total_num = epupdnum
        else:
            if row.eptotal == 0:
                total_num = 1
            else:
                total_num = int(row.eptotal)

        return row.aid, row.vip, row.fee, row.isCoupon, total_num, int(row.eqlen)

    movie_type = movie_type.rdd.map(map).toDF(['aid', 'vip', 'fee', 'isCoupon', 'total_num', 'eqlen'])

    return movie_type