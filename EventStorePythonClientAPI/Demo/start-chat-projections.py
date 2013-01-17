from ClientAPI.EventStoreClient import *

es_client = Client(ip_address = "127.0.0.1")
projections = es_client.projections
stream_id = "chat-1"
es_client.create_stream(stream_id)
f = open("query.js", "r")
name= "chat-on-projections"
query = f.read();
f.close()
try:
    projections.post_continuous(query, name, enabled = "1", emit = "1")
    print "Started"
except BaseException, ex:
    print "Not started: ", str(ex.message)