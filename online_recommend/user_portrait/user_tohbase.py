from utils.save_tohbase import RetTohbase


class UserTohbase(RetTohbase):
    def __init__(self, table, func, _insert):
        """
        :param table: 【str】 保存到hbase的表名
        :param func:  【object】 计算相似度的函数名
        :param _insert: 【object】 写入hbase的语句函数
        """
        super(UserTohbase, self).__init__(table, func, _insert)
        self.size = 10
        """
        table_prefix_separator参数默认为b'_'使得table_prefix只是table前缀，
        改为 b':'后table_prefix参数变为hbase的命名空间 (因为hbase中命名空间和表之间是冒号连接的)
        """
        self.table_prefix = 'user'
        self.table_prefix_separator = b':'


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
    # 保存用户画像词和权重
    RetTohbase(table='user_profile', func=get_cos_sim, _insert=_insert)

    # namespace:table         row_key             key:value
    # user:user_profile     user:user_id     {protrait:cate_id:topic:weight}