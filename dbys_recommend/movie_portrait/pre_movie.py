from utils import m_spark, get_filter_data, movie_portrait_db


def pre_movie_data():
    """
    1365065 条数据
    预处理电影数据
    :return: 【dataframe】
    """
    movie_data = m_spark.sql("select id, cid, title, cat, act, director, area, desc, year, score,\
                            create_date, tag1, tag2 from dbys.tp_page")

    def map(row):
        try:
            cate = row.cat + ',' + ','.join(eval(row.tag1))
        except:
            cate = row.cat
        try:
            cate = cate + ',' + ','.join(eval(row.tag2))
        except:
            pass

        create_date = int(''.join(row.create_date.split()[0].split('-')))

        return row.id, row.cid, row.title, cate, row.act, row.director, row.area, row.desc, \
               row.year, row.score, create_date

    movie_data = movie_data.rdd.map(map) \
        .toDF(['id', 'cid', 'title', 'cate', 'actor', 'director', 'area', 'desc', 'year', 'score', 'create_time'])

    movie_data = get_filter_data(movie_data)

    return movie_data


def cut_words(movie_df, cut_field):
    import re
    def map(row):
        words = []
        # '!', '"', '#', '$', '%', '&', '\(', '\)', '\*', '\+', ',', '-', '\.', '/', ':', ';', '<', '=', '>', '^\w+'
        # '\?', '@', '\[', '\\', '\]', '^', '_', '`', '\{', '\|', '\}', '~', '\t', '\n', '\x97', '\x96', '”', '“',
        filters = ['国内', '暂无', '国外', '未知', '大陆', '\\', '\.']

        if cut_field == 'cate':
            words = list(set(''.join(row.cate.split()).split(',')))
        elif cut_field == 'actor':
            sentence = re.sub("|".join(filters), "", row.actor)
            words = list(set(''.join(sentence.split()).split(',')))
        elif cut_field == 'director':
            sentence = re.sub("|".join(filters), "", row.director)
            words = list(set(''.join(sentence.split()).split(',')))
        elif cut_field == 'area':
            words = list(set(''.join(row.area.split()).split(',')))
        try:
            words.remove('')
        except:
            pass

        return row.id, row.cid, row.title, words, row.create_time

    movie_df = movie_df.rdd.map(map).toDF(['movie_id', 'cate_id', 'title', 'words', 'create_time'])

    return movie_df


if __name__ == '__main__':
    pass
