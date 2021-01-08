from movie_recall import get_cos_sim, get_lsh_similar
from utils import RetTohbase


def _insert(partition, bat):
    """
    用于向hbase中插入数据， 每个数据表字段不同需要单独写put语句
    :param partition: 【partition】
    :param bat: 【batch】
    :return:
    """
    for row in partition:
        # bat.put(str(row.datasetA.movie_id).encode(),
        #         {"similar:{}".format(row.datasetB.movie_id).encode(): b"%0.4f" % (row.EucDistance)})
        bat.put(str(row.movie_id).encode(),
                {"similar:{}".format(row.movie_id2).encode(): b"%0.4f" % (row.cos_sim)})



if __name__ == '__main__':
    # 保存lsh相似度
    # RetTohbase(table='movie_lsh', func=get_lsh_similar, _insert=_insert)

    # 保存cos相似度
    RetTohbase(table='movie_cos', func=get_cos_sim, _insert=_insert)