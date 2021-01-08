# client.py
import sys
sys.path.append('../gen-py')

from absystem import ABTest #引入客户端类
from absystem.ttypes import *
from thrift import Thrift 
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

try:
    #建立socket
    transport = TSocket.TSocket('47.114.87.224', 10001)
    #选择传输层，这块要和服务端的设置一致
    transport = TTransport.TBufferedTransport(transport)
    #选择传输协议，这个也要和服务端保持一致，否则无法通信
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    #创建客户端
    client = ABTest.Client(protocol)
    transport.open()

    msg = client.get(ABTestReq('uid=fe6543fe9221211ga'))
    print("server - " + msg)
    #关闭传输
    transport.close()
#捕获异常
except Thrift.TException as ex:
    print("%s" % (ex.message))
