from utils import m_spark, get_filter_data, movie_portrait_db


def pre_movie_data():
    """
    1365065 条数据
    预处理电影数据
    :return: 【dataframe】
    """
    song_df = m_spark.sql("select kugou_song_id, song_name, singer_id, singer_name, album_id, album_name, duration,\
                            created_at from movie.song")
    # album_df = m_spark.sql("select kugou_album_id, album_name, created_at from movie.album")
    lyric_df = m_spark.sql("select kugou_song_id, content from movie.lyric")

    filters = ['\[.*?\]']
    import re
    def map(row):
        content = re.sub("|".join(filters), "", row.content)
        return row.kugou_song_id, content

    lyric_df = lyric_df.rdd.map(map).toDF(['kugou_song_id', 'content'])
    song_df = song_df.join(lyric_df, on='kugou_song_id', how='left')

    return song_df


def cut_words():
    import re
    song_df = m_spark.sql("select * from {}.movie_feature".format(movie_portrait_db))
    def map(row):
        song_words = []
        singer_words = []
        # '!', '"', '#', '$', '%', '&', '\(', '\)', '\*', '\+', ',', '-', '\.', '/', ':', ';', '<', '=', '>',
        # '\?', '@', '\[', '\\', '\]', '^', '_', '`', '\{', '\|', '\}', '~', '\t', '\n', '\x97', '\x96', '”', '“',
        # '^\w+'
        filters = ['\(', '\)', '\+', '（', '）']   # 108598454 有中文括号
        filters_2 = ['!', '"', '#', '$', '%', '&', '\(', '\)', '\*', '\+', ',', '-', '\.', '/', ':', ';', '<', '=',
                     '>', '\?', '@', '\[', '\\', '\]', '^', '_', '`', '\{', '\|', '\}', '~', '\t', '\n', '\x97',
                     '\x96', '”', '“']

        sentence = re.sub("|".join(filters), ",", row.song_name)
        song_words = [i.strip() for i in sentence.split(',') if i != '']

        singer_words = [i.strip() for i in row.singer_name.split(',') if i != '']

        album_words = [row.album_name.strip()]

        if row.content:
            content = re.sub("|".join(filters_2), " ", row.content).lower()
        else:
            content = ''

        return row.kugou_song_id, song_words, row.singer_id, singer_words, row.album_id, album_words, content

    song_df = song_df.rdd.map(map).toDF(['song_id', 'song', 'singer_id', 'singer', 'album_id', 'album', 'content'])

    return song_df



if __name__ == '__main__':
    pass
