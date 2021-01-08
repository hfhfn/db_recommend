from utils import movie_recall_db


def get_movie_recall(spark, cate_id, k, minDCS):

    for group in range(k):
        data = spark.sql("select * from {}.movie_cos_sim_{}_k{}m{}_{}".format(movie_recall_db, cate_id, k, minDCS, group))
        if group == 0:
            tmp_data = data
        else:
            tmp_data = tmp_data.union(data)

    return tmp_data


if __name__ == '__main__':
    pass