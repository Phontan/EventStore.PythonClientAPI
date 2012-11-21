import sys
sys.path.append("D:\\apps\\EventStore.PythonClientAPI\\EventStorePythonClientAPI\\HttpClientAPI(python27)\\libs")
import tornado.ioloop
from ClientAPI import ClientAPI
from Event import Event

import uuid

def onStreamCreatedSuccess(msg):
    tornado.ioloop.IOLoop.instance().stop()
    print("Create stream code: "+str(msg))
def onStreamCreatedFaild(msg):
    tornado.ioloop.IOLoop.instance().stop()
    print("Create stream code: "+str(msg))
def deleteSuccess(msg):
    tornado.ioloop.IOLoop.instance().stop()
    print("Delete stream code: "+str(msg))
def deleteFaild(msg):
    tornado.ioloop.IOLoop.instance().stop()
    print("Delete stream code: "+str(msg))
def onAppendEventSuccess(msg):
    print("Append to stream code: "+str(msg))
    tornado.ioloop.IOLoop.instance().stop()
def onAppendEventFaild(msg):
    print("Append to stream code: "+str(msg))
    tornado.ioloop.IOLoop.instance().stop()
def readEvent_handle(msg):
    print("Read event msg: "+str(msg))
    tornado.ioloop.IOLoop.instance().stop()
def onSth():
    print("sth");

client = ClientAPI();
#client.CreateStreamAsync("SomeNewStream1", "", onStreamCreatedSuccess, onStreamCreatedFaild)
#client.DeleteStreamAsync("SomeNewStream",deleteSuccess, deleteFaild)
#client.AppendToStreamAsync("SomeNewStream", Event("EventData", "EventMetadata"),onAppendEventSuccess, onAppendEventFaild)
#client.ReadEventAsync("SomeNewStream", 2,readEvent_handle, readEvent_handle)
#client.ReadStreamEventsBackwardAsync("SomeNewStream", 5,5,readEvent_handle, readEvent_handle)
client.ReadAllEventsForwardAsync(0,-0,10,readEvent_handle,readEvent_handle)
tornado.ioloop.IOLoop.instance().start()