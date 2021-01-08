import sys

sys.path.append('../gen-py')

from profile_mp import Profile  # 引入客户端模块
from profile_mp.ttypes import *
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
import logging
from pymysql import connect


class Client(object):
    def __init__(self, hosts, port, timeout=20.0, conn_timeout=1):
        self.index = 0
        self.clients = []
        for host in hosts:
            # transport = SocketPool.TSocketPool(host, port, timeout, conn_timeout)
            transport = TSocket.TSocket(host, port)
            transport = TTransport.TFramedTransport(transport)
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            # Profile 是生成的模块，不同的服务需要导入替换
            client = Profile.Client(protocol)
            transport.open()
            self.clients.append((client, transport))

    def _reconnect(self, transport):

        try:
            transport.close()
            transport.open()
        except Exception as e:
            logging.exception(e)

    def __del__(self):

        for client, transport in self.clients:
            transport.close()

    def _get_client(self):

        self.index = (self.index + 1) % len(self.clients)
        return self.clients[self.index]

    def put_content(self, req):

        client, transport = self._get_client()
        try:
            return client.put_content_profiles(req, {"name": "test"})
        except Exception as e:
            print(e)
            self._reconnect(transport)

    def get_content(self, req):

        client, transport = self._get_client()
        try:
            return client.get_content_profiles_ex(req, {"name": "test"})
        except Exception as e:
            logging.exception(e)
            self._reconnect(transport)
            return None

    def put_user(self, req):

        client, transport = self._get_client()
        try:
            return client.put_user_profiles(req, {"name": "val"})
        except Exception as e:
            print(e)
            self._reconnect(transport)

    def get_user(self, req):

        client, transport = self._get_client()
        try:
            return client.get_user_profiles_ex(req, {"name": "test"})
        except Exception as e:
            logging.exception(e)
            self._reconnect(transport)
            return None

    def put_author(self, req):
        client, transport = self._get_client()

        try:
            return client.put_author_profiles(req, {"name": "test"})
        except Exception as e:
            print(e)
            self._reconnect(transport)

    def get_author(self, req):

        client, transport = self._get_client()
        try:
            return client.get_author_aid_profiles_ex(req, {"name": "test"})
        except Exception as e:
            logging.exception(e)
            self._reconnect(transport)
            return None


class GenReq(object):

    def __init__(self, uid="fe5143fe817bf3c1", aid=0, cid=12, itid=394269, i_duration=7147, i_category='1969:1',
                 i_quality=8.9, i_p_count=0, i_title='美国:1, 队长:1', i_keyword='', i_actor='克里斯埃文斯:1',
                 i_year=2011, i_vip=1, i_country='美国', i_author_id=1111, u_age=2, u_gender=0, u_city=8234,
                 u_register_time=20201101, u_vip=0):
        # 主参
        self.uid = uid
        # aid表示author_id, i64类型
        self.aid = aid
        # cid表示content_id, i64类型
        self.cid = cid
        # Content参数
        # 默认参数 美国队长
        self.itid = itid
        self.i_duration = i_duration
        self.i_category = i_category
        self.i_quality = i_quality
        self.i_p_count = i_p_count
        self.i_title = i_title
        self.i_keyword = i_keyword
        self.i_actor = i_actor
        self.i_year = i_year
        self.i_vip = i_vip
        self.i_country = i_country
        self.i_author_id = i_author_id
        # User参数
        self.u_age = u_age
        self.u_gender = u_gender
        self.u_city = u_city
        self.u_register_time = u_register_time
        self.u_vip = u_vip

    def put_content_req(self):
        req = ContentProfilePutReq(content_list=[
            Content(itid=self.itid, i_duration=self.i_duration, i_category=self.i_category,
                    i_quality=self.i_quality, i_title=self.i_title,
                    i_keyword=self.i_keyword, i_actor=self.i_actor, i_year=self.i_year,
                    i_vip=self.i_vip, i_country=self.i_country)
        ])
        return req

    def get_content_req(self):
        req = ContentProfileGetReq(cid_list=[self.cid])
        return req

    def put_autor_req(self):
        req = AuthorProfilePutReq(author_list=[
            Author(aid=self.aid)
        ])
        return req

    def get_autor_req(self):
        req = AuthorProfileAidGetReq(aid_list=[self.aid])
        return req

    def put_user_req(self):
        req = UserProfilePutReq(user_list=[
            User(uid=self.uid,
                 u_register_time=self.u_register_time, u_vip=self.u_vip)
        ])
        return req

    def get_user_req(self):
        req = UserProfileGetReq(uid_list=[self.uid])
        return req


def get_data(table):
    # 创建Connection连接
    host = 'mysql'
    db = 'movie'
    conn = connect(host=host, port=3306, user='root', password='mysql', database=db, charset='utf8')
    # 获得Cursor对象
    cs = conn.cursor()

    # 安全的方式
    # 构造参数列表
    # params = [table]
    # 执行select语句，并返回受影响的行数：查询所有数据
    sql = "select * from {};".format(table)
    count = cs.execute(sql)
    # 注意：
    # 如果要是有多个参数，需要进行参数化
    # 那么params = [数值1, 数值2....]，此时sql语句中有多个%s即可
    # %s 不需要带引号

    # 打印受影响的行数
    print(count)
    # 获取查询的结果
    # result = cs.fetchone()
    result = cs.fetchall()
    # print(result)
    # for line in csl.fetchall():
    #	print(line)
    #	break
    # 关闭Cursor对象
    cs.close()
    # 关闭Connection对象
    conn.close()
    return result


def main():
    req = GenReq()
    # print(req)

    host = "47.114.87.224"
    port = 10002
    client = Client([host], port)
    # put
    put_content_rsp = client.put_content(req.put_content_req())
    put_user_rsp = client.put_user(req.put_user_req())
    put_author_rsp = client.put_author(req.put_autor_req())

    # get
    get_content_rsp = client.get_content(req.get_content_req())
    get_user_rsp = client.get_user(req.get_user_req())
    get_author_rsp = client.get_author(req.get_autor_req())

    print(put_user_rsp, get_user_rsp)
    print(put_author_rsp, get_author_rsp)
    print(put_content_rsp, get_content_rsp)


def get_profile(get_data, param):
    # 连接服务端
    host = "47.114.87.224"
    port = 10002
    client = Client([host], port)

    if get_data == 'user':
        uid = str(param)
        req = GenReq(uid=uid)
        get_user_rsp = client.get_user(req.get_user_req())
        print(get_user_rsp)
    else:
        cid = int(param)
        req = GenReq(cid=cid)
        get_content_rsp = client.get_content(req.get_content_req())
        print(get_content_rsp)


def put_profile(put_data):
    import gc
    from tqdm import tqdm
    # 连接服务端
    host = "47.114.87.224"
    port = 10002
    client = Client([host], port)

    if put_data == 'user':
        # put_user
        user_data = get_data('r_db_user_profile')
        for params in tqdm(user_data, desc='导入user画像'):
            uid = str(params[2])
            u_register_time = params[3]
            u_vip = params[4]
            req = GenReq(uid=uid, u_register_time=u_register_time, u_vip=u_vip)
            # print(req)
            put_user_rsp = client.put_user(req.put_user_req())
            #print(put_user_rsp)
        del user_data
        gc.collect()

    else:
        # put_item
        item_data = get_data('r_db_item_profile')
        for params in tqdm(item_data, desc='导入item画像'):
            itid = int(params[0])
            i_duration = int(params[1])
            i_category = params[2]
            try:
                i_quality = float(params[3])
            except:
                i_quality = 0.0
            i_title = params[4]
            i_keyword = params[5]
            i_actor = params[6]
            try:
                i_year = int(params[7])
            except:
                i_year = 0
            i_vip = int(params[8])
            i_country = params[9]
            req = GenReq(itid=itid, i_duration=i_duration, i_category=i_category,
                         i_quality=i_quality, i_title=i_title, i_keyword=i_keyword,
                         i_actor=i_actor, i_year=i_year, i_vip=i_vip, i_country=i_country)
            put_content_rsp = client.put_content(req.put_content_req())
            #print(put_content_rsp)
        del item_data
        gc.collect()


if __name__ == "__main__":
    # data = get_data('db_asset')
    # print(data[0])
    # main()
    #put_profile('user')
    #get_profile('user', 10)
    #put_profile('item')
    get_profile('item', sys.argv[1])



