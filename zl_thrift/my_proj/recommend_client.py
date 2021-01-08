import sys

sys.path.append('../gen-py')

from zlyq import Zreco  # 引入客户端类
from zlyq.ttypes import Req, Rsp
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
        self.client = Zreco.Client(protocol)
        self.client.transport = transport

    def get_item_list(self, req):
        self.client.transport.open()
        result = self.client.get_item_list(req)
        self.client.transport.close()
        return result


def gen_req():
    uid_type, mac_id, user_id = 14, '4C:91:7A:00:A4:AF', 61814950
    app_id = '1'
    channel_id = 1972
    count = 20
    context_info = ''
    context_feature = '{"chnid":1972, "time":1500000000}'
    ab_params = '{}'

    return Req(
        uid_type=uid_type,
        udid=mac_id,
        user_id=user_id,
        app_id=app_id,
        channel_id=channel_id,
        count=count,
        context_info=context_info,
        context_feature=context_feature,
        abtest_parameters=ab_params,
        content_type=1,
    )


def main():
    req = gen_req()
    print(req)

    host = "47.114.87.224"
    port = 7199
    client = Client(host, port)
    rsp = client.get_item_list(req)

    print(rsp)


if __name__ == "__main__":
    main()
