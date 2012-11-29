import sys
sys.path.append("D:\\apps\\EventStore.PythonClientAPI\\EventStorePythonClientAPI\\HttpClientAPI(python27)\\libs")
import tornado.ioloop
from ClientAPI import ClientAPI
from Event import Event

import threading

import uuid

def readEvent_handle(msg):
    print("Read event msg: "+str(msg))

client = ClientAPI();
client.CreateStreamAsync("SomeNewStream", " ", readEvent_handle, readEvent_handle)
#client.DeleteStreamAsync("SomeNewStream",deleteSuccess, deleteFaild)
#client.AppendToStreamAsync("SomeNewStream", Event({1:"a", 2:"b", 3:{31:"aa"}}, "EventMetadata"),readEvent_handle, readEvent_handle)
#dummy_event = threading.Event()
#dummy_event.wait()
#print("END")
#client.ReadEventAsync("SomeNewStream", 2,readEvent_handle, readEvent_handle)
#client.ReadStreamEventsBackwardAsync("SomeNewStream", 60,40,readEvent_handle, readEvent_handle)
#client.ReadAllEventsForwardAsync(0,-0,10,readEvent_handle,readEvent_handle)
#tornado.ioloop.IOLoop.instance().start()
#threading.currentThread().join()