import sys
sys.path.append("D:\\apps\\EventStore.PythonClientAPI\\EventStorePythonClientAPI\\HttpClientAPI(python27)\\libs")
import tornado.ioloop
from ClientAPI import ClientAPI
from Event import Event

import threading

import uuid

from Event import *
from ClientJsonSerelizationOption import *
import sys
sys.path.append("D:\\apps\\EventStore.PythonClientAPI\\EventStorePythonClientAPI\\HttpClientAPI(python27)\\libs");
from bodyLibs import *
from answerLibs import *
from AsyncRequestSender import *
import Ensure
from threading import*
from ReadEventsData import *
import time


def readEvent_handle(msg):
    print("Read event msg: "+str(msg))
    #client.Resume()

def p(msg):
    print(msg)

##__tornadoHttpSender = TornadoHttpSender();
##
##print('fuck1')
##__tornadoHttpSender.SendAsync("http://google.com", "GET", None, None, lambda x: p('fuck callback1'))
##__tornadoHttpSender.SendAsync("http://google.com", "GET", None, None, lambda x: p('fuck callback2'))
##__tornadoHttpSender.SendAsync("http://google.com", "GET", None, None, lambda x: p('fuck callback2'))
##__tornadoHttpSender.SendAsync("http://google.com", "GET", None, None, lambda x: p('fuck callback2'))
##__tornadoHttpSender.SendAsync("http://google.com", "GET", None, None, lambda x: p('fuck callback2'))
##__tornadoHttpSender.SendAsync("http://google.com", "GET", None, None, lambda x: p('fuck callback2'))
##__tornadoHttpSender.SendAsync("http://google.com", "GET", None, None, lambda x: p('fuck callback2'))
##
##
##thread = Thread(target=tornado.ioloop.IOLoop.instance().start).start()
##print('fuck2')

client = ClientAPI();
client.CreateStream("SomeNewStream","", readEvent_handle, readEvent_handle)
#client.Wait();
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