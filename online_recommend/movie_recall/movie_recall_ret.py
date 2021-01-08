

def get_movie_recall(spark, cate_id, k, minDCS):

    for group in range(k):
        data = spark.sql("select * from movie_recall.movie_cos_sim_{}_k{}m{}_{}".format(cate_id, k, minDCS, group))
        if group == 0:
            tmp_data = data
        else:
            tmp_data = tmp_data.union(data)

    return tmp_data


if __name__ == '__main__':
    pass