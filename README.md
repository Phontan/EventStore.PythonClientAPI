EventStore.PythonClientAPI
==========================
Simple client for EventStore(https://github.com/EventStore/EventStore).
<ul><li><a href="#short-description">Short description</a></li>
<li><a href="#installation">Installation</a></li>
<li><a href="#implementation">Implementation</a></li>
<li><a href="#functionality-description">Functionality description</a></li>
<li><a href="#hello-world">Hello World</a></li>
</ul>

<h4>Short description</h4>
СlientAPI is a python http client for Event Store. 
It contains the necessary Event Store options, allows you to feel flexibility of EventStore. 
You can easy create, delete streams, write, read events forward and backward, from special stream or from all streams.
Also ClientAPI supports projections. Just use property projections in ClientAPI and you can easily
post, get, enable, disable projection etc. <br/><br/>

Current API is wrtten for Python 2.7 and tested on Windows 7 x64 and Ubuntu Linux x64, 
but should work fine everywhere where Python 2.7 is supported.

<h4>Installation</h4>
<ul><li>Install tornado web server(http://pypi.python.org/pypi/tornado)</li>
<li>Download zip from github(http://github.com/Phontan/EventStore.PythonClientAPI)</li>
<li>Unpackage it and open console in this folder as admin(sudo).</li>
<li>Write <i>python setup.py install</i></li></ul>
Now you can use ClientAPI. You can check if it works by typing <i>from ClientAPI import *</i>. 
If there are no errors everything is fine.

<h4>Implementation</h4>
To implement ClientAPI we chose http protocol. We use http tornado client as one of the fastest 
python http libs. We have sync and async modes for almost all methods. Async methods' callbacks will not be triggered 
unless you wait for them explicitly by calling method <i>wait()</i> from ClientAPI. This method will block main thread, 
to release it call <i>resume()</i> in one of your callbacks.<br/>

<h4>Functionality description</h4>
As mentioned above, we have sync and async modes. If your operation is successful,
sync mode returns to you some answer, and async mode calls your <i>on_success</i> callback. If your operation failed,
sync mode throws an exception, and async calls your <i>on_failed</i> callback. If you pass some not expected arguments 
both modes throw error. <br/>
All methods discribed below have also asyncronous mode: method name ends with <i>_async</i>, 
and have two additional arguments(<i>on_success</i> and <i>on_failed</i>).
These two arguments should be functions with one argument, or lambdas.

In all methods <i>stream_id</i> is a string, <i>expected_version</i>, <i>event_number</i>, <i>start_position</i> 
and <i>count</i> are integers, <i>prepare_position</i> and <i>commit_position</i> are long integers

To create stream in Event Store use <i>create_stream(stream_id, metadata="")</i><br>
metadata can be of any type <br/>

To delete stream from Event Store use <i>delete_stream(stream_id, expected_version=-2)</i> <br/>

To append events to some stream use <i>append_to_stream(stream_id, events, expected_version=-2)</i> <br/>
<i>events</i> is either an instanse of class <i>Event.WriteEvent</i>, or list of such instances. 
To create an instance of class WriteEvent use
<i>WriteEvent(data, metadata="", event_id = None, event_type=None, is_json = False)</i> <br/>
event_id should a GUID. By default event_id is a random GUID and event_type is python's type of data argument. <br/>

When reading events from Event Store you receive instace(s) of class <i>ReadEvent</i> class which has fields 
<i>data</i>, <i>metadata</i>, <i>event_type</i> and <i>event_number</i>.

To read one event use <i>read_event(stream_id, event_number)</i>.<br/>
If operation succeeded, this method returns instance of <i>ReadEvent</i> class.

To read stream events use one of methods:<br>
<i>read_stream_events_backward(stream_id, start_position, count)</i><br>
<i>read_stream_events_forward(stream_id, start_position, count)</i><br>
These methods return you list of <i>ReadEvent</i> objects.<p>

To read from all streams use<br>
<i>read_all_events_backward(prepare_position, commit_position, count)</i><br>
<i>read_all_events_forward(prepare_position, commit_position, count)</i><br>
These methods return you object, with fields <i>prepare_position</i>, <i>commit_position</i> and <i>events</i>,
where events is a list of <i>ReadEvent</i> objects.<br>

<h4>Hello World</h4>

Make sure you are running compatible version of Event Store on default address(127.0.0.1:2113) and run following code:

<pre>
from ClientAPI.ClientAPI import *
from ClientAPI.Event import *

client = ClientAPI("127.0.0.1", 2113)
stream_id = "some_stream"
client.create_stream(stream_id)
client.append_to_stream(stream_id, WriteEvent("hello, Event Store!"))
events = client.read_stream_events_backward(stream_id, 5, 100)
for event in events:
  print "Type: {0}, Data: {1}".format(event.event_type, event.data)
client.delete_stream(stream_id)
</pre>

After running this code you should receive output similar to 
<pre>
Type: &lt;type 'str' &gt;, Data: hello, Event Store!
Type: $stream-created, Data: 
</pre>

