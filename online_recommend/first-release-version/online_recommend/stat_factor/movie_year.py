from utils import MovieDataApp

spark = MovieDataApp().spark


def get_movie_year_factor():
    movie_data = spark.sql("select * from movie.db_asset").select('id', 'year')
    import numpy as np
    from datetime import datetime
    now_year = datetime.today().year

    def extract_movie_year(row):
        try:
            movie_year = int(row.year)
            if movie_year <= 0 or movie_year > 2020:
                movie_year = now_year - 5   # 认为没有year的电影可能不是特别新的和重要的，适当的 -5
        except:
            movie_year = now_year - 5   # 对没有year或者year异常的（0或者大于2020）给一个适合的year（自选一个）
        deltayear = now_year - movie_year

        # 衰减因子
        # year_exp = 1 / (np.logs(deltaday + 1) + 1)
        # 采用指数加权，调整指数范围大概在 2^0.1 ~ 2^5 （1970~2020）之间，即 1 ~ 32 倍，选2为底防止倍数放大太多
        # 指数加权，可以使年代久的电影权重偏低更多
        year_exp = 1 / (2 ** (deltayear / 10))   # 为了使指数域在0.1~5，对 deltayear 除以10
        return row.id, row.year, round(float(year_exp), 4)

    tmp_table = movie_data.rdd.map(extract_movie_year).toDF(['movie_id', 'year', 'factor'])

    return tmp_table