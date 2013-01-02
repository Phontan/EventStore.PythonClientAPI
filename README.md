EventStore.PythonClientAPI
==========================
Simply client for EventStore(https://github.com/EventStore/EventStore).

1. Installation 
To install Client API just go to EventStorePythonClientAPI in console and write python setup.py install
Note that tornado web server are required for this product(http://www.tornadoweb.org/).

2. Short Description 
ClientAPI allow you to feel flexibility of EventStore. It contains the necessary EventStore options. 
You can easy create, delete streams, write reade events in all orders, from special stream of from all.
Also clientAPI has progections methods. Just use property projections in clientAPI, and you can easy use
post, get, enable, disable and many other options of projections.

3. Implementation
To implement ClientAPI we choese http protocol. We use http tornado client as one of the fastest 
http python libs. We have sync and async modes for most methods. If you want wait answer on your async method
you must start ioloop(we have method wait() in clientAPI), and stop it after responce come(we also have 
method resume() in clientAPI). Dont forget call resume, because ioloop locks your thread. Projections have only
sync mode, so it easy to use it. To write events you should use WriteEvent class from file Event. 
Only data field is required. If you are reading events, clientAPI will return you ReadEvent object,
or list of ReadEvent objects.

4. Functionality description

To create stream in Event Store use create_stream or create_stream_async method from ClientAPI.
create_stream(stream_id, metadata="")
stream_id should be type of string object. Metadata is not required argument, and by default is empty string.
This method does not return value if operation is success and throws an exception if operation failed.
create_stream_async(stream_id, metadata="", on_success = None, on_failed = None)
This method have two additional arguments. On success and on failed can be functions with one argument, or lambdas.

To delete stream from Event Store use delete_stream and delete_stream_async methods.
delete_stream(stream_id, expected_version=-2) and delete_stream_async(stream_id, expected_version=-2, on_success = None, on_failed = None)
works in same way as create_stream and create_stream_async.

If you want to push events in the stream, use append_to_stream(stream_id, events, expected_version=-2) and
append_to_stream_async(stream_id, events, expected_version=-2, on_success = None, on_failed = None), where events 
is or one instanse of object Event.WriteEvent, of list of this objects. Class WriteEvent has fields data, metadata="", 
event_id = None, event_type=None, is_json = False.

To read one event use method read_event(stream_id, event_number) or 
read_event_async(self,stream_id, event_number, on_success = None, on_failed = None).
If reading success, this methods return ReadEvent object, with fields data, metadata, event_type and event_number.

You can easy read stream events in different orders. Just use on of methods:
read_stream_events_backward(stream_id, start_position, count), 
read_stream_events_backward_async(stream_id, start_position, count, on_success = None, on_failed = None),
read_stream_events_forward(stream_id, start_position, count), or 
read_stream_events_forward_async(stream_id, start_position, count, on_success = None, on_failed = None).
This methods returns you list of ReadEvent objects.

To read from all use following:
read_all_events_backward(prepare_position, commit_position, count), 
read_all_events_backward_async(prepare_position, commit_position, count, on_success = None, on_failed = None),
read_all_events_forward(prepare_position, commit_position, count) or
read_all_events_forward_async(prepare_position, commit_position, count, on_success = None, on_failed = None).
This methods return you object, with fields prepare_position, commit_position and events, where events - list
of ReadEvent objects.


4. Simple usage
from ClientAPI import ClientAPI
from ClientAPI.Events import *

client_api = ClientAPI("127.0.0.1", 2113)
stream_id = "some_my_stream"
client_api.create_stream(stream_id)
client_api.append_to_stream(stream_id, WriteEvent("some fake data"))
read_data = client_api.read_event(stream_id, 1)
#if everething was fine, should print "some fake data"
print read_event.data
