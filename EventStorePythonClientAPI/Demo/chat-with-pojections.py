import sys
from ClientAPI.EventStoreClient import *
from ClientAPI.Event import *
import msvcrt

def call_back(response):
    print ''
    print response.data

es_client = Client(ip_address = "127.0.0.1")
projections = es_client.projections
stream_id = "chat-1"
print "Enter your name:"
user_name=""
done = False
while not done:
    if msvcrt.kbhit():
        temp = msvcrt.getch()
        if temp!="\r":
            sys.stdout.write(temp)
            user_name+=temp
        else:
            done = True
es_client.subscribe("$projections-chat-on-projections-state", call_back)
es_client.append_to_stream(stream_id, WriteEvent({"user": user_name}, event_type = "login", is_json = True))
message = ""
done = False
while not done:
    if msvcrt.kbhit():
        temp = msvcrt.getch()
        if temp!="\r":
            sys.stdout.write(temp)
            message+=temp
        else:
            sys.stdout.write("\n")
            es_client.append_to_stream_async(stream_id, WriteEvent({"user":user_name, "message": message}, event_type = "message", is_json = True))
            message=""
es_client.append_to_stream(stream_id, WriteEvent({"user":user_name}, event_type = "logout", is_json = True))