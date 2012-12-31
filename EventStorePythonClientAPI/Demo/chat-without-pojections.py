import sys
from ClientAPI.ClientAPI import *
from ClientAPI.Event import *
import msvcrt

def call_back(response):
    print""
    print "user say: ", response.data

num = 0
done = False
es_client = ClientAPI()
stream_id = "chat-3"
es_client.subscribe(stream_id, call_back)
message = ""
while not done:
    if msvcrt.kbhit():
        temp = msvcrt.getch()
        if temp!="z":
            sys.stdout.write(temp)
            message+=temp
        else:
            es_client.append_to_stream_async(stream_id, WriteEvent(message))
            message=""