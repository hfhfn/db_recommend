from utils import MovieDataApp


spark = MovieDataApp().spark

def get_movie_score_factor():
    score_df = spark.sql("select id, title, cid, year, score from movie.db_asset")

    import pyspark.sql.functions as fn
    tmp = score_df.where('score>0').agg(fn.mean('score').alias('mean')).first()
    mean = tmp.mean
    print(mean)
    max = 10

    def map(row):
        score = row.score
        if not score:
            score = mean    # 7.2左右
        deltaScore = max - score
        # 衰减因子
        import numpy as np
        # 电影分数大于7分，作对数衰减
        if score >= 7:
            score_exp = 1 / (np.log(deltaScore + 1) + 1)    # log是以e为底的对数
        # 电影分数小于7分，作指数衰减
        # 为了放大倍数，使低分的电影权重衰减更快，选用指数函数，又不希望衰减倍数过大，所以选2为底（小底数放大倍数小）
        else:
            score_exp = 1 / (2 ** (deltaScore - 1))    # deltaScore-1 为了调和对数和指数在7分左右的衰减差不多

        return row.id, row.title, row.cid, row.score, round(float(score_exp), 4)

    tmp_df = score_df.rdd.map(map).toDF(['aid', 'title', 'cid', 'score', 'factor'])

    from pyspark.sql import Window
    nice_df = tmp_df.withColumn("sort_num",
                fn.row_number().over(Window.partitionBy("cid").orderBy(fn.desc('factor'))))

    return nice_df     # aid, title, cid, score, factor, sort_num


if __name__ == '__main__':
    get_movie_score_factor()
    pass