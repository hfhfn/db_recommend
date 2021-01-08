# client.py
import sys
sys.path.append('../gen-py')

from zlyq import Zreco #引入客户端类
from zlyq.ttypes import Req, Rsp
from thrift import Thrift 
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

try:
    #建立socket
    transport = TSocket.TSocket('47.114.87.224', 7199)
    #选择传输层，这块要和服务端的设置一致
    transport = TTransport.TFramedTransport(transport)
    #选择传输协议，这个也要和服务端保持一致，否则无法通信
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    #创建客户端
    client = Zreco.Client(protocol)
    transport.open()
    req = Req(uid_type=14, udid='fe6543fe9221211ga', user_id=61814950, app_id='1', channel_id=1972, count=20, context_info='', context_feature='{"chnid":1972, "time":1500000000}', abtest_parameters="{}", content_type=1)
    print(req)
    msg = client.get_item_list(req)
    print(msg)
    print(Rsp(msg))
    #关闭传输
    transport.close()
#捕获异常
except Thrift.TException as ex:
    print("%s" % (ex.message))
