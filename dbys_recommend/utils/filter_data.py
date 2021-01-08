

def get_filter_data(data):

    movie_filter = ['像奖', '指南', '影节', '斯卡', '直击', '鲜看', '剧集', '音乐', '纪念', '电影', '体验']

    def movie_map(row):
        title = row.title.strip().lower()
        # 取标题前2个字符，判断相同title
        # title3 = title[:2]
        # 取标题后2个字符，判断异常电影 =》 奥斯卡金像奖， 观影指南， 上影节， 奥斯卡...
        title4 = title[-2:]
        title5 = title[:1] + title[3:4]
        if title4 in movie_filter or title5 in ['【】', '第届'] or '《' in list(title):
            return None, None, None, None, None, None, None, None, None, None, None

        return row.id, row.cid, row.title, row.cate, row.actor, row.director, row.area, row.desc, row.year, \
               row.score, row.create_time

    tv_filter = ['映礼', '手记', '专访', '看版', '春晚', '老歌', '合集', '编版', '制版', '华版', '战队', '纪实',
                 '回顾', '剧场', '大赏', 'og', '日常', '货版', '演唱', 'p版', '录片', '盛典', '速看',
                 'mv', '外篇', '视频', '唱会']

    def tv_map(row):
        title = row.title.strip().lower()
        # 取标题前2个字符，判断相同title
        title3 = title[:4]
        # 取标题后2个字符，判断异常电影 =》 奥斯卡金像奖， 观影指南， 上影节， 奥斯卡...
        title4 = title[-2:]
        if title4 in tv_filter or '《' in list(title) or title3 == 'test':
            return None, None, None, None, None, None, None, None, None, None, None

        return row.id, row.cid, row.title, row.cate, row.actor, row.director, row.area, row.desc, row.year, \
               row.score, row.create_time

    # def other_map(row):
    #
    #     return row.id, row.cid, row.title, row.cate, row.actor, row.director, row.area, row.desc, row.year, \
    #            row.score, row.create_time

    # 得到一个空的 DataFrame
    df = data.select('id', 'cid', 'title', 'cate', 'actor', 'director', 'area', 'desc', 'year', 'score', 'create_time')\
        .where('id = -1')

    try:
        movie_df = data.where('cid=1969').rdd.map(movie_map).toDF(
            ['id', 'cid', 'title', 'cate', 'actor', 'director', 'area', 'desc', 'year', 'score', 'create_time']).dropna()
        # movie_df.show()
    except Exception as e:
        movie_df = df
        print('movie_df  Error: ', e)
    try:
        tv_df = data.where('cid=1970').rdd.map(tv_map).toDF(
            ['id', 'cid', 'title', 'cate', 'actor', 'director', 'area', 'desc', 'year', 'score', 'create_time']).dropna()
        # tv_df.show()
    except Exception as e:
        tv_df = df
        print('tv_df  Error: ', e)
    try:
        other_df = data.where('cid<>1969 and cid<>1970')#.select(
            # 'id', 'cid', 'title', 'cate', 'actor', 'director', 'area', 'desc', 'year', 'score', 'create_time')
        # other_df.show()
    except Exception as e:
        other_df = df
        print('other_df  Error: ', e)

    data = movie_df.union(tv_df).union(other_df)
    return data