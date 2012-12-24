from collections import deque
import thread
import time
import sys
from urllib import quote
from Implementation import Ensure, ReadEventsData
from Implementation.Projections import *
from Implementation.AsyncRequestSender import TornadoHttpSender
from Implementation.SubscribeAllInfo import SubscribeAllInfo
from Implementation.SubscribeInfo import SubscribeInfo
from Implementation.SyncResponse import SyncResponse
from Implementation.ClientJsonSerelizationOption import *
from Implementation.Body.CreateStreamRequestBody import CreateStreamRequestBody
from Implementation.Body.DeleteStreamRequestBody import DeleteStreamRequestBody
from Implementation.Body.AppendToStreamRequestBody import AppendToStreamRequestBody
from Implementation.ReturnClasses.FailedAnswer import FailedAnswer
from Implementation.ReturnClasses.AllEventsAnswer import AllEventsAnswer


class ClientAPI():
  def __init__(self, ip_address="127.0.0.1", port = 2113):
    self._base_url = "http://{0}:{1}".format(ip_address, str(port))
    self._headers = {"content-type" : "application/json", "accept" : "application/json", "extensions" : "json"}
    self._read_batch_size = 20
    self._tornado_http_sender = TornadoHttpSender()
    self._subscribers = []
    self._subscribers_thread = thread.start_new_thread(self._handle_subscribers, ())
    self._subscribers_all_thread = thread.start_new_thread(self._handle_subscribers_all, ())
    self._should_subscribe_all=False
    self.projections = Projections(ip_address, port)


  def resume(self):
    IOLoop.instance().stop()
  def wait(self):
    IOLoop.instance().start()


  def _sync_success(self, response):
    self.resume()
    response = SyncResponse(True, response)
    return response
  def _sync_failed(self, response):
    self.resume()
    response = SyncResponse(False, response)
    return response


########################################################################################################################


  def create_stream(self, stream_id, metadata=""):
    queue = deque()
    on_success = lambda x: queue.append(self._sync_success(x))
    on_failed = lambda x: queue.append(self._sync_failed(x))
    self.create_stream_async(stream_id, metadata, on_success, on_failed)
    self.wait()
    result = queue.popleft()
    if result.success:
      return
    else:
      raise result.response

  def create_stream_async(self, stream_id, metadata="", on_success = None, on_failed = None):
      if on_success is None: on_success = lambda x: self._do_nothing(x)
      if on_failed is None: on_failed = lambda x: self._do_nothing(x)
      self._start_create_stream(stream_id, metadata, on_success, on_failed)

  def _start_create_stream(self, stream_id, metadata, on_success, on_failed):
    Ensure.is_not_empty_string(stream_id, "stream_id")
    Ensure.is_string(metadata, "metadata")
    Ensure.is_function(on_success, "on_success")
    Ensure.is_function(on_failed, "on_failed")
    body = to_json(CreateStreamRequestBody(stream_id, metadata))
    url = "{0}/streams".format(self._base_url)
    self._tornado_http_sender.send_async(url, "POST", self._headers, body, lambda x: self._create_stream_callback(x, on_success, on_failed))

  def _create_stream_callback(self, response, on_success, on_failed):
    if response.code==201:
      on_success(response)
    else:
      on_failed(FailedAnswer(response.code, response.error.message))



  def delete_stream(self, stream_id, expected_version=-2):
    queue = deque()
    on_success = lambda x: queue.append(self._sync_success(x))
    on_failed = lambda x: queue.append(self._sync_failed(x))
    self.delete_stream_async(stream_id,expected_version, on_success, on_failed)
    self.wait()
    result = queue.popleft()
    if result.success:
      return
    else:
      raise result.response

  def delete_stream_async(self, stream_id, expected_version=-2, on_success = None, on_failed = None):
      if on_success is None: on_success = lambda x: self._do_nothing(x)
      if on_failed is None: on_failed = lambda x: self._do_nothing(x)
      self._start_delete_stream(stream_id,expected_version, on_success, on_failed)

  def _start_delete_stream(self,stream_id, expected_version, on_success, on_failed, ):
    Ensure.is_not_empty_string(stream_id, "stream_id")
    Ensure.is_number(expected_version, "expected_version")
    Ensure.is_function(on_success, "on_success")
    Ensure.is_function(on_failed, "on_failed")

    stream_id = quote(stream_id)
    url = "{0}/streams/{1}".format(self._base_url, stream_id)
    body = to_json(DeleteStreamRequestBody(expected_version))
    self._tornado_http_sender.send_async(url, "DELETE", self._headers, body,
        lambda x: self._delete_stream_callback(x, on_success, on_failed))

  def _delete_stream_callback(self, response, on_success, on_failed):
    if response.code==204:
      on_success(response)
    else:
      on_failed(FailedAnswer(response.code,response.error.message))



  def append_to_stream(self,stream_id, events, expected_version=-2):
    queue = deque()
    on_success = lambda x: queue.append(self._sync_success(x))
    on_failed = lambda x: queue.append(self._sync_failed(x))
    self.append_to_stream_async(stream_id, events,expected_version, on_success, on_failed)
    self.wait()
    result = queue.popleft()
    if result.success:
      return
    else:
      raise result.response

  def append_to_stream_async(self,stream_id, events, expected_version=-2, on_success = None, on_failed = None):
      if on_success is None: on_success = lambda x: self._do_nothing(x)
      if on_failed is None: on_failed = lambda x: self._do_nothing(x)
      self._start_append_to_stream(stream_id, events, expected_version, on_success, on_failed)

  def _start_append_to_stream(self,stream_id, events, expected_version, on_success, on_failed):
    Ensure.is_not_empty_string(stream_id, "stream_id")
    Ensure.is_number(expected_version, "expected_version")
    Ensure.is_function(on_success, "on_success")
    Ensure.is_function(on_failed, "on_failed")

    stream_id = quote(stream_id)
    if type(events) is not list:
      events = [events]
    body = to_json(AppendToStreamRequestBody(expected_version, events))
    url = "{0}/streams/{1}".format(self._base_url,stream_id)
    self._tornado_http_sender.send_async(url,"POST", self._headers, body,
        lambda x: self._append_to_stream_callback(x, on_success, on_failed))

  def _append_to_stream_callback(self, response, on_success,on_failed):
    if response.code==201:
      on_success(response)
    else:
      on_failed(FailedAnswer(response.code,response.error.message))



  def read_event(self,stream_id, event_number):
    queue = deque()
    on_success = lambda x: queue.append(self._sync_success(x))
    on_failed = lambda x: queue.append(self._sync_failed(x))
    self.read_event_async(stream_id, event_number, on_success, on_failed)
    self.wait()
    result = queue.popleft()
    if result.success:
      return result.response
    else:
      raise result.response

  def read_event_async(self,stream_id, event_number, on_success = None, on_failed = None):
      if on_success is None: on_success = lambda x: self._do_nothing(x)
      if on_failed is None: on_failed = lambda x: self._do_nothing(x)
      self._start_read_event(stream_id, event_number, on_success, on_failed)

  def _start_read_event(self,stream_id, event_number, on_success, on_failed):
    Ensure.is_not_empty_string(stream_id, "stream_id")
    Ensure.is_greater_number_then(0,event_number, "event_number")
    Ensure.is_function(on_success, "on_success")
    Ensure.is_function(on_failed, "on_failed")

    stream_id = quote(stream_id)
    url = "{0}/streams/{1}/event/{2}".format(self._base_url, stream_id, str(event_number))
    self._tornado_http_sender.send_async(url, "GET", self._headers, None,
        lambda x: self._read_event_callback(x, on_success, on_failed))

  def _read_event_callback(self, response, on_success, on_failed):
    if response.code!=200:
      on_failed(FailedAnswer(response.code,response.error.message))
      return
    responseContent = response.body
    event = json.loads(responseContent)
    on_success(event)



  def read_stream_events_backward(self, stream_id, start_position, count):
    queue = deque()
    on_success = lambda x: queue.append(self._sync_success(x))
    on_failed = lambda x: queue.append(self._sync_failed(x))
    self.read_stream_events_backward_async(stream_id, start_position, count, on_success, on_failed)
    self.wait()
    result = queue.popleft()
    if result.success:
      return result.response
    else:
      raise result.response

  def read_stream_events_backward_async(self, stream_id, start_position, count, on_success = None, on_failed = None):
      if on_success is None: on_success = lambda x: self._do_nothing(x)
      if on_failed is None: on_failed = lambda x: self._do_nothing(x)
      self._start_read_stream_events_backward(stream_id, start_position, count, on_success, on_failed)

  def _start_read_stream_events_backward(self, stream_id, start_position, count, on_success, on_failed):
    Ensure.is_not_empty_string(stream_id, "stream_id")
    Ensure.is_greater_number_then(-1,start_position, "start_position")
    Ensure.is_greater_number_then(1, count, "count")
    Ensure.is_function(on_success, "on_success")
    Ensure.is_function(on_failed, "on_failed")

    stream_id = quote(stream_id)
    events = []
    batch_counter=0
    params = ReadEventsData
    params.stream_id = stream_id
    params.start_position = start_position
    params.count = count
    params.batch_counter = batch_counter
    params.events = events
    self._read_batch_events_backward(params, on_success, on_failed)

  def _read_batch_events_backward(self, params, on_success, on_failed, events_count=None):
    if events_count is not None and events_count < self._read_batch_size:
      on_success(params.events)
      return
    if params.batch_counter < params.count:
      if params.count < params.batch_counter + self._read_batch_size:
        params.batch_langth = params.count - params.batch_counter
      else :
        params.batch_langth = self._read_batch_size
      url = self._base_url + "/streams/{0}/range/{1}/{2}".format(
          params.stream_id,
          str(params.start_position-params.batch_counter),
          str(params.batch_langth))
      params.batch_counter+=self._read_batch_size
      self._tornado_http_sender.send_async(url, "GET", self._headers, None,
          lambda x: self.read_stream_events_backward_async_callback(x, params, on_success, on_failed))
    else:
      on_success(params.events)

  def read_stream_events_backward_async_callback(self, response, params, on_success, on_failed):
    if response.code!=200:
      on_failed(FailedAnswer(response.code, "Error occur while reading batch: " + response.error.message))
      return
    try:
      response = json.loads(response.body)
      if len(response["entries"])==0:
        if len(params.events)!=0:
          on_success(params.events)
          return
        url = "{0}/streams/{1}".format(self._base_url, params.stream_id)
        self._tornado_http_sender.send_async(url, "GET", self._headers, None,
            lambda x: self._on_read_events_first_response_entries_empty(x, params, on_success, on_failed))
        return
      batch_events = []
      for uri in response["entries"]:
        url = uri["links"]
        for ur in url:
          try:
            if ur["type"] == "application/json":
              url = ur["uri"]
              self._tornado_http_sender.send_async(url, "GET", self._headers, None,
                  lambda x: self._event_read_callback(x, params,batch_events, on_success, on_failed,len(response["entries"])))
              break
          except:
            continue
    except:
      on_failed(FailedAnswer(response.code, "Error occur while process batch: " + response.error.message))
      return

  def _on_read_events_first_response_entries_empty(self, response, params, on_success, on_failed):
    if response.code!=200:
      on_failed(FailedAnswer(response.code, "Error occur while reading first page: " + response.error.message))
      return
    response = json.loads(response.body)
    if len(response["entries"])==0:
      on_success(params.events)
      return
    last_event_number = int(response["entries"][0]["id"].split("/")[-1])
    if  params.count < params.start_position - last_event_number:
      on_success(params.events)
      return
    params.count = params.count + last_event_number - params.start_position
    params.start_position = last_event_number
    params.batch_counter = 0
    self._read_batch_events_backward(params, on_success, on_failed)

  def _event_read_callback(self, response, params, batch_events, on_success, on_failed, events_count):
    if response.code !=200:
      on_failed(FailedAnswer(response.code, response.error.message))
      return
    try:
      batch_events.append(json.loads(response.body))
      if len(batch_events)==events_count:
          batch_events = sorted(batch_events, key=lambda ev: ev["eventNumber"], reverse=True)
          params.events+=batch_events
          self._read_batch_events_backward(params, on_success, on_failed, events_count)
    except:
        on_failed(FailedAnswer(response.code, "Error occure while reading event: " + response.body))
        return



  def read_stream_events_forward(self, stream_id, start_position, count):
    queue = deque()
    on_success = lambda x: queue.append(self._sync_success(list(reversed(x))))
    on_failed = lambda x: queue.append(self._sync_failed(x))
    self.read_stream_events_forward_async(stream_id, start_position, count, on_success, on_failed)
    self.wait()
    result = queue.popleft()
    if result.success:
      return result.response
    else:
      raise result.response

  def read_stream_events_forward_async(self, stream_id, start_position, count, on_success = None, on_failed = None):
      new_start_position = start_position+count-1
      if new_start_position > sys.maxint:
          count -= (new_start_position-sys.maxint)
          new_start_position = sys.maxint

      self._start_read_stream_events_backward(stream_id, int(new_start_position), int(count), on_success, on_failed)



  def read_all_events_backward(self, prepare_position, commit_position, count):
    queue = deque()
    on_success = lambda x: queue.append(self._sync_success(x))
    on_failed = lambda x: queue.append(self._sync_failed(x))
    self._start_read_all_events_backward(prepare_position, commit_position, count, on_success, on_failed)
    self.wait()
    result = queue.popleft()
    if result.success:
      return result.response
    else:
      raise result.response

  def read_all_events_backward_async(self, prepare_position, commit_position, count, on_success = None, on_failed = None):
      if on_success is None: on_success = lambda x: self._do_nothing(x)
      if on_failed is None: on_failed = lambda x: self._do_nothing(x)
      self._start_read_all_events_backward(prepare_position, commit_position, count, on_success, on_failed)

  def _start_read_all_events_backward(self, prepare_position, commit_position, count, on_success, on_failed):
    Ensure.is_number(prepare_position, "prepare_position")
    Ensure.is_number(commit_position, "commit_position")
    Ensure.is_greater_number_then(1, count, "count")
    Ensure.is_function(on_success, "on_success")
    Ensure.is_function(on_failed, "on_failed")

    events = []
    batch_counter = 0
    params = ReadEventsData
    params.prepare_position = prepare_position
    params.commit_position = commit_position
    params.count = count
    params.batch_counter = batch_counter
    params.events = events
    self._read_batch_all_events_backward(params, on_success, on_failed)

  def _read_batch_all_events_backward(self, params, on_success, on_failed, events_count=None):
    if events_count is not None and events_count < self._read_batch_size:
      on_success(AllEventsAnswer(params.events, params.prepare_position, params.commit_position))
      return
    if params.batch_counter < params.count:
      hexStartPosition = self._convert_to_hex(params.prepare_position) + self._convert_to_hex(params.commit_position)
      if params.count < params.batch_counter + self._read_batch_size:
        params.batch_langth = params.count - params.batch_counter
      else :
        params.batch_langth = self._read_batch_size
      url = "{0}/streams/$all/before/{1}/{2}".format(self._base_url, hexStartPosition, str(params.batch_langth))
      params.batch_counter+=self._read_batch_size
      self._tornado_http_sender.send_async(url, "GET", self._headers, None,
          lambda x: self._read_all_events_backward_page_callback(x, params, on_success, on_failed))
    else:
      on_success(AllEventsAnswer(params.events, params.prepare_position, params.commit_position))

  def _read_all_events_backward_page_callback(self, response, params, on_success, on_failed):
    if response.code!=200:
      on_failed(FailedAnswer(response.code,"Error occur while reading links: " + response.error.message))
    read_line = response.body
    body = json.loads(read_line)
    params.prepare_position = self._get_prepare_position(body["links"][4]["uri"])
    params.commit_position = self._get_commit_position(body["links"][4]["uri"])

    try:
      if body["entries"] == []:
        if len(params.events)!=0:
          on_success(AllEventsAnswer("", 0,0 ))
        else:
          self._start_read_all_events_backward(-1,-1, params.count, on_success, on_failed)
        return
      events_count=len(body["entries"])
      batch_events = {}
      url_number_dictionary = {}
      event_number = 0
      for uri in body["entries"]:
        url = uri["links"]
        for ur in url:
          try:
            if ur["type"] == "application/json":
              url = ur["uri"]
              url_number_dictionary[url] = event_number
              event_number+=1
              self._tornado_http_sender.send_async(url, "GET", self._headers, None,
                  lambda x: self._read_all_events_backward_callback(x, params, batch_events, on_success, on_failed,events_count, url_number_dictionary))
              break
          except:
            continue
    except:
      on_failed(FailedAnswer(response.code,response.error.message))
      return

  def _read_all_events_backward_callback(self, response, params, batch_events, on_success, on_failed, events_count, url_number_dictionary):
    if response.code!=200:
      on_failed(FailedAnswer(response.code, response.error.message))
      return
    try:
      batch_events[url_number_dictionary[response.request.url]]=json.loads(response.body)
      if len(batch_events)==events_count:
        i = 0
        while i<events_count:
          params.events.append(batch_events[i])
          i+=1
        self._read_batch_all_events_backward(params, on_success, on_failed, events_count)
    except:
      on_failed(FailedAnswer(response.code,"Error occure while reading event: "+response.error.message))



  def read_all_events_forward(self, prepare_position, commit_position, count):
    queue = deque()
    on_success = lambda x: queue.append(self._sync_success(x))
    on_failed = lambda x: queue.append(self._sync_failed(x))
    self.read_all_events_forward_async(prepare_position, commit_position, count, on_success, on_failed)
    self.wait()
    result = queue.popleft()
    if result.success:
      return result.response
    else:
      raise result.response

  def read_all_events_forward_async(self, prepare_position, commit_position, count, on_success = None, on_failed = None):
      if on_success is None: on_success = lambda x: self._do_nothing(x)
      if on_failed is None: on_failed = lambda x: self._do_nothing(x)
      self._start_read_all_events_forward(prepare_position, commit_position, count, on_success, on_failed)

  def _start_read_all_events_forward(self, prepare_position, commit_position, count, on_success, on_failed):
    Ensure.is_greater_number_then(-1, prepare_position, "prepare_position")
    Ensure.is_greater_number_then(-1, commit_position, "commit_position")
    Ensure.is_greater_number_then(1, count, "count")
    Ensure.is_function(on_success, "on_success")
    Ensure.is_function(on_failed, "on_failed")

    events = []
    batch_counter = 0
    params = ReadEventsData
    params.prepare_position = prepare_position
    params.commit_position = commit_position
    params.count = count
    params.batch_counter = batch_counter
    params.events = events
    self._read_batch_all_events_forward(params, on_success, on_failed)

  def _read_batch_all_events_forward(self, params, on_success, on_failed, events_count=None):
    if events_count!=None and events_count<self._read_batch_size:
      on_success(AllEventsAnswer(params.events, params.prepare_position, params.commit_position))
      return
    if params.batch_counter<params.count:
      hexStartPosition = self._convert_to_hex(params.prepare_position) + self._convert_to_hex(params.commit_position)
      if params.batch_counter+self._read_batch_size>params.count:
        params.batch_langth = params.count - params.batch_counter
      else :
        params.batch_langth = self._read_batch_size
      url = "{0}/streams/$all/after/{1}/{2}".format(self._base_url, hexStartPosition, str(params.batch_langth))
      params.batch_counter+=self._read_batch_size
      self._tornado_http_sender.send_async(url, "GET", self._headers, None, lambda x: \
      self._read_all_events_forward_page_callback(x, params, on_success, on_failed))
    else:
      on_success(AllEventsAnswer(params.events, params.prepare_position, params.commit_position))

  def _read_all_events_forward_page_callback(self, response, params, on_success, on_failed):
    if response.code!=200:
      on_failed(FailedAnswer(response.code,"Error occur while reading links: " + response.error.message))
    read_line = response.body
    body = json.loads(read_line)
    params.prepare_position = self._get_prepare_position(body["links"][3]["uri"])
    params.commit_position = self._get_commit_position(body["links"][3]["uri"])
    try:
      if body["entries"] == []:
        on_success(AllEventsAnswer(params.events, params.prepare_position, params.commit_position ))
        return
      events_count=len(body["entries"])
      batch_events = {}
      url_number_dictionary = {}
      event_number = 0
      for uri in body["entries"]:
        url = uri["links"]
        for ur in url:
          try:
            if ur["type"] == "application/json":
              url = ur["uri"]
              url_number_dictionary[url] = event_number
              event_number+=1
              self._tornado_http_sender.send_async(url, "GET", self._headers, None,
                  lambda x: self._read_all_events_forward_callback(x, params, batch_events, on_success, on_failed,events_count, url_number_dictionary))
              break
          except:
            continue
    except:
      on_failed(FailedAnswer(response.code,response.error.message))
      return

  def _read_all_events_forward_callback(self, response, params, batch_events, on_success, on_failed,events_count, url_number_dictionary):
    if response.code !=200:
      on_failed(FailedAnswer(response.code,response.error.message))
      return
    try:

      batch_events[url_number_dictionary[response.request.url]]=json.loads(response.body)
      if len(batch_events)==events_count:
        i = events_count-1
        while i>=0:
          params.events.append(batch_events[i])
          i-=1
        self._read_batch_all_events_forward(params, on_success, on_failed, events_count)
    except:
      on_failed(FailedAnswer(response.code, "Error occure while reading event: " + response.error.message))



  def _get_prepare_position(self, link):
    position = link.split("/")[6]
    result =int(position[0:16], 16)
    return result

  def _get_commit_position(self, link):
    position = link.split("/")[6]
    result =int(position[16:32], 16)
    return result

  def _convert_to_hex(self, number):
    if number<0:
      return"ffffffffffffffff"
    hexVal = hex(number)
    number = str(hexVal).split("x")[1]
    while len(number)!=16:
      number = "0"+number
    return number



  def subscribe(self, stream_id, callback, start_from_begining = False):
    start_position = 0 if start_from_begining else self._get_stream_event_position(stream_id) + 1
    self._subscribers.append(SubscribeInfo(stream_id,start_position, callback))

  def _handle_subscribers(self):
    while True:
      if len(self._subscribers)>0:
        processed_streams=0
        for i in range(len(self._subscribers)):
            #print "in handle subscribes. streamId is {0}, start position is {1}, events count is ".format(self._subscribers[i].stream_id, self._subscribers[i].last_position, sys.maxint)
            self.read_stream_events_forward_async(self._subscribers[i].stream_id, self._subscribers[i].last_position, sys.maxint,
                lambda s: self._handle_subscribers_success(s, len(self._subscribers), processed_streams, i),
                lambda s: self._handle_subscribers_failed(len(self._subscribers), processed_streams))
        self.wait()
      time.sleep(1)

  def _handle_subscribers_success(self, response, stream_count, processed_streams, subscriber_index):
      processed_streams+=1
      self._subscribers[subscriber_index].last_position += len(response)
      for i in reversed(response):
          self._subscribers[subscriber_index].callback(i)

      if processed_streams==stream_count:
          self.resume()

  def _handle_subscribers_failed(self, stream_count, processed_streams):
      processed_streams+=1
      if processed_streams==stream_count:
          self.resume()

  def unsubscribe(self, stream_id):
    for i in self._subscribers:
        if i.stream_id == stream_id:
            self._subscribers.remove(i)



  def subscribe_all(self, callback, start_from_begining = False):
    start_position = 0 if start_from_begining else -1

    self.__subscribersAll=SubscribeAllInfo(start_position, start_position, callback)
    self._should_subscribe_all=True

  def _handle_subscribers_all(self):
    while True:
      if self._should_subscribe_all:
        if self.__subscribersAll.commit_position ==-1:
          answer = self.read_all_events_backward(self.__subscribersAll.prepare_position,self.__subscribersAll.commit_position, 1)
        else:
          try:
            answer = self.read_all_events_forward(self.__subscribersAll.prepare_position,self.__subscribersAll.commit_position, sys.maxint-1)
            for i in answer.events:
              self.__subscribersAll.callback(i)
          except:
            time.sleep(0.1)


        self.__subscribersAll.commit_position = int(answer.commit_position)
        self.__subscribersAll.prepare_position = int(answer.prepare_position)

      time.sleep(1)

  def unsubscribe_all(self):
    self._should_subscribe_all = False



  def _get_stream_event_position(self,stream_id):
    try:
        events = self.read_stream_events_backward(stream_id, -1, 1)
        return int(events[0]["eventNumber"])
    except:
        return 0

  def _do_nothing(self, x):
      a=0