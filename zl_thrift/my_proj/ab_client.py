import sys

sys.path.append('../gen-py')

from absystem import ABTest  # 引入客户端类
from absystem.ttypes import *
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol


class Client(object):
    def __init__(self, host, port, timeout=20.0, conn_timeout=1):
        # transport = SocketPool.TSocketPool(host, port, timeout, conn_timeout)
        transport = TSocket.TSocket(host, port)
        transport = TTransport.TFramedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        self.client = ABTest.Client(protocol)
        self.client.transport = transport

    def get(self, req):
        self.client.transport.open()
        result = self.client.get(req)
        self.client.transport.close()
        return result


def gen_req():
    import sys
    uid = sys.argv[1]
    params = {"app_id":"1"}
    ext = ''

    return ABTestReq(
        uid=uid,
        params=params,
        ext=ext,
    )


def main():
    req = gen_req()
    # print(req)

    host = "47.114.87.224"
    port = 10001
    client = Client(host, port)
    rsp = client.get(req)

    print(rsp)


if __name__ == "__main__":
    main()
