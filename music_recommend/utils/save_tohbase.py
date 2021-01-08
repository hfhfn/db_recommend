

class RetTohbase(object):

    def __init__(self, table, func, _insert):
        """
        :param table: 【str】 保存到hbase的表名
        :param func:  【object】 计算相似度的函数名
        :param _insert: 【object】 写入hbase的语句函数
        """
        self.size= 3
        self.host = "hadoop-master"
        """
        table_prefix_separator参数默认为b'_'使得table_prefix只是table前缀，
        改为 b':'后table_prefix参数变为hbase的命名空间 (因为hbase中命名空间和表之间是冒号连接的)
        """
        self.table_prefix = 'movie'
        self.table_prefix_separator = b':'
        self.table = table
        self._insert = _insert
        similar = func()
        # from functools import partial
        # add_save_hbase = partial(save_hbase, table, _insert)
        similar.foreachPartition(self.save_hbase)

    def save_hbase(self, partition):

        import happybase
        # table_prefix是指定命名空间, size 线程数
        pool = happybase.ConnectionPool(size=self.size, host=self.host,
                        table_prefix=self.table_prefix, table_prefix_separator=self.table_prefix_separator)
        with pool.connection() as conn:
            """
            删除表，创建表，启用表， 
            此处因为 函数被 foreachPartition 方法调用， 每个partition就要执行一遍所以报错
            最好提前创建好表
            """
            # conn.delete_table('movie_lsh', disable=True)
            # conn.create_table('movie_lsh', {'similar': dict(max_versions=3)})
            # conn.enable_table("movie_lsh")

            # 建立表的连接
            table = conn.table(self.table)
            # print(conn.tables())

            # 一条一条的写
            # for row in partition:
            #     if row.datasetA.movie_id == row.datasetB.movie_id:
            #         pass
            #     else:
            #     table.put(str(row.datasetA.movie_id).encode(),
            #             {"similar:{}".format(row.datasetB.movie_id).encode(): b"%0.4f" % (row.EucDistance)})

            # 一个batch一个batch写入 （减少IO写入次数，加快速度）
            with table.batch(batch_size=10) as bat:
                self._insert(partition, bat)
                # for row in partition:
                    # # bat.put(str(row.datasetA.movie_id).encode(),
                    # #         {"similar:{}".format(row.datasetB.movie_id).encode(): b"%0.4f" % (row.EucDistance)})
                    # bat.put(str(row.movie_id).encode(),
                    #         {"similar:{}".format(row.movie_id2).encode(): b"%0.4f" % (row.cos_sim)})

            # 查看数据
            # for key, value in table.scan(row_prefix='xxx'):
            for key, value in table.scan(row_start='1000', row_stop='1050'):
                print(key, value)
            # 手动关闭所有的连接
            conn.close()
