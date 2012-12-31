EventStore.PythonClientAPI
==========================
Simply client for EventStore(https://github.com/EventStore/EventStore).

1. Installation 
To install Client API just go to EventStorePythonClientAPI in console and write python setup.py install
Note that tornado web server are required for this product(http://www.tornadoweb.org/).

2. Description 
ClientAPI allow you to feel flexibility of EventStore. It contains the necessary EventStore options. 
You can easy create, delete streams, write read events in all orders, from special stream of from all.
Also clientAPI has progections methods. Just use property projections in clientAPI, and you can easy use
post, get, enable, disable and many other options of projections.

3. Implementation
To implement ClientAPI we chose http protocol. We use http tornado client as one of the fastest 
http python libs. We have sync and async modes for most methods. If you want wait answer on your async method
you must start ioloop(we have method wait() in clientAPI), and stop it after responce come(we also have 
method resume() in clientAPI). Dont forget call resume, because ioloop locks your thread. Projections have only
sync mode, so it easy to use it. To write events you should use WriteEvent class from file Event. 
Only data field is required. If you are reading events, clientAPI will return you ReadEvent object,
or list of ReadEvent objects.

