# server.py
import socket
import sys
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
#print(BASE_DIR + '/gen-py')
sys.path.append(BASE_DIR + '/gen-py') 

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

from helloword import HelloWorld 
from helloword.ttypes import *

class HelloWorldHandler:  
    def ping(self):   
        return "pong"   
    def say(self, msg):
        ret = "Received: " + msg
        print(ret)    
        return ret
#创建服务端
handler = HelloWorldHandler()
processor = HelloWorld.Processor(handler)
#监听端口
transport = TSocket.TServerSocket("127.0.0.1", 9090)
#选择传输层
tfactory = TTransport.TBufferedTransportFactory()
#选择传输协议
pfactory = TBinaryProtocol.TBinaryProtocolFactory()
#创建服务端 
server = TServer.TSimpleServer(processor, transport, tfactory, pfactory) 
print("Starting thrift server in python...")
server.serve()
print("done!")
