import sys
import glob
import logging
sys.path.append('../gen-py')
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from profile_mp import Profile
from profile_mp.ttypes import *
import random
import time


class Client(object):
    def __init__(self, hosts, port):
        self.index = 0
        self.clients = []
        for host in hosts:
            transport = TSocket.TSocket(host, port)
            transport = TTransport.TFramedTransport(transport)
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
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
            return client.get_content_profiles_ex(req, {"name":"test"})
        except Exception as e:
            logging.exception(e)
            self._reconnect(transport)
            return None

    def put(self, req):
        client, transport = self._get_client()
        try:
            return client.put_user_profiles(req, {"name": "val"})
        except Exception as e:
            print(e)
            self._reconnect(transport)

    def get(self, req):
        client, transport = self._get_client()
        try:
            return client.get_user_profiles_ex(req, {"name":"test"})
        except Exception as e:
            logging.exception(e)
            self._reconnect(transport)
            return None


if __name__ == "__main__":
    c = Client(["47.114.87.224"], 10002)

    # test user
    uid = "fe5143fe817bf3c1"

    # put
    req = UserProfilePutReq(user_list=[
        User(uid=uid, u_age=30, u_gender=0, u_city=10392, u_register_time=202000901, u_vip=1)
    ])
    print(req)
    print(c.put(req))

    # get
    req = UserProfileGetReq(uid_list = [uid])
    print(c.get(req))


    ## content
    #req = ContentProfilePutReq()
    #us = []
    #us.append(Content(cid=int(i.strip().split()[0]), c_like_num=30, c_author_id=random.randint(10000, 20000)))
    #req.content_list = us
    #print(c.put_content(req))

    #req = ContentProfileGetReq()
    #req.cid_list = [452511297787949056]
    #print(req)
    #print(c.get_content(req))

