EventStore.PythonClientAPI
==========================
Simply client for EventStore(https://github.com/EventStore/EventStore).
<ul><li><a href="#short-description">Short description</a></li>
<li><a href="#nstallation">Installation</a></li>
<li><a href="#implementation">Implementation</a></li>
<li><a href="#functionality-description">Functionality description</a></li></ul>

<h4>Short description.</h4><p> СlientAPI is a python http client for Event Store(https://github.com/EventStore/EventStore). 
It contains the necessary Event Store options, allows you to feel flexibility of EventStore. 
You can easy create, delete streams, write, read events in all orders, from special stream or from all.
Also ClientAPI supports projections. Just use property projections in ClientAPI and you can easy
post, get, enable, disable projection.

<h4>Installation.</h4><p>
<ul><li>Install tornado web server(http://pypi.python.org/pypi/tornado)</li>
<li>Download zip from github(http://github.com/Phontan/EventStore.PythonClientAPI)</li>
<li>Unpackage it and open console in this folder as admin(sudo).</li>
<li>Write <i>python setup.py install</i></li></ul>
Now you can use ClientAPI.

<h4>Implementation.</h4><p> To implement ClientAPI we choose http protocol. We use http tornado client as one of the fastest 
python http libs. We have sync and async modes for almost all methods. If you want to wait answer on your async method
you must call method <i>wait()</i> from ClientAPI, and call <i>resume()</i> after responce come. Dont forget call <i>resume()</i>, 
because method <i>start()</i> locks your thread, what makes async methods dangerous to use.<br>
Projections have only sync mode, so it easy to use it. To write events you should use <i>WriteEvent</i> class from file Event. 
Only <i>data</i> field is required. If you are reading events, clientAPI will return you <i>ReadEvent</i> object,
or list of <i>ReadEvent</i> objects.

<h4>Functionality description.</h4><p> As I wrote above, we have sync and async modes. If your operation is success,
sync mode return to you some answer, and async mode calls your <i>on_success</i> callback. If your operation failed,
sync mode throws an exception, and async calls your <i>on_failed</i> callback. If you pass some not expected arguments 
both modes throws error.<p>
To create stream in Event Store use <i>create_stream or create_stream_async</i> method from ClientAPI.<br>
<i>create_stream(stream_id, metadata="")</i>:<br>
Here stream_id should be type of string object. Metadata is not required argument, and by default is empty string.
<i>create_stream_async(stream_id, metadata="", on_success = None, on_failed = None):</i>
This method have two additional arguments. On success and on failed can be functions with one argument, or lambdas.<p>
To delete stream from Event Store use <i>delete_stream</i> and <i>delete_stream_async</i> methods.<br>
<i>delete_stream(stream_id, expected_version=-2)</i> and <i>delete_stream_async(stream_id, expected_version=-2, on_success = None, on_failed = None)</i>
works in same way as <i>create_stream</i> and <i>create_stream_async</i>.<p>
If you want to push events in the stream use <i>append_to_stream(stream_id, events, expected_version=-2)</i> and
<i>append_to_stream_async(stream_id, events, expected_version=-2, on_success = None, on_failed = None)</i>, where <i>events</i> 
is or one instanse of object <i>Event.WriteEvent</i>, or list of these objects. Class WriteEvent has fields data, metadata="", 
event_id = None, event_type=None, is_json = False.<p>
To read one event use method <i>read_event(stream_id, event_number)</i> or 
<i>read_event_async(self,stream_id, event_number, on_success = None, on_failed = None)</i>.
If reading success, this methods return <i>ReadEvent</i> object, with fields data, metadata, event_type and event_number.<p>
You can easy read stream events in different orders. Just use one of methods:<br>
<i>read_stream_events_backward(stream_id, start_position, count)</i><br>
<i>read_stream_events_backward_async(stream_id, start_position, count, on_success = None, on_failed = None)</i><br>
<i>read_stream_events_forward(stream_id, start_position, count)</i><br>
<i>read_stream_events_forward_async(stream_id, start_position, count, on_success = None, on_failed = None)</i>.<br>
These methods returns you list of <i>ReadEvent</i> objects.<p>
To read from all use following:<br>
<i>read_all_events_backward(prepare_position, commit_position, count)</i><br>
<i>read_all_events_backward_async(prepare_position, commit_position, count, on_success = None, on_failed = None)</i><br>
<i>read_all_events_forward(prepare_position, commit_position, count)</i><br>
<i>read_all_events_forward_async(prepare_position, commit_position, count, on_success = None, on_failed = None)</i>.<br>
These methods return you object, with fields <i>prepare_position</i>, <i>commit_position</i> and <i>events</i>, where events - list
of <i>ReadEvent</i> objects.
