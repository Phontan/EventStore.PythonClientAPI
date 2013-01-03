
from ClientAPI.ClientAPI import *
from ClientAPI.Event import *

client = ClientAPI("127.0.0.1", 2113)
stream_id = "some_stream"
client.create_stream(stream_id)
client.append_to_stream(stream_id, WriteEvent("hello, Event Store!"))
events = client.read_stream_events_backward(stream_id, 5, 100)
for event in events:
  print "Type: {0}, data: {1}".format(event.event_type, event.data)
client.delete_stream(stream_id)