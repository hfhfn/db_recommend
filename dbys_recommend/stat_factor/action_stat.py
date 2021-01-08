from utils import u_spark, user_pre_db


def get_action_stat():
    """
    统计用户行为数据
    :return:
    """
    action = u_spark.sql(
        'select user_id, cate_id, datetime, click, top, play_time from {}.merge_action'.format(user_pre_db))
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