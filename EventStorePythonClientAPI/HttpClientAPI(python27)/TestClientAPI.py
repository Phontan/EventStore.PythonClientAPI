import sys
sys.path.append("D:\\apps\\EventStore.PythonClientAPI\\EventStorePythonClientAPI\\HttpClientAPI(python27)\\libs")
import tornado.ioloop
from ClientAPI import ClientAPI
from Event import Event



def readEvent_handle(msg):
    print("Read event msg: "+str(msg))

def p(msg):
    print(msg)


client = ClientAPI();
#client.CreateStream("SomeNewStream","")
#client.DeleteStreamAsync("SomeNewStream",deleteSuccess, deleteFaild)
client.AppendToStream("SomeNewStream", Event({"a":1, "b":"b"}, "metadata"))