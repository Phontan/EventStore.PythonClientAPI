from collections import deque
import thread
import time
import sys
from Implementation import Ensure, ReadEventsData
from Implementation.AsyncRequestSender import TornadoHttpSender
from Implementation.SubscribeAllInfo import SubscribeAllInfo
from Implementation.SubscribeInfo import SubscribeInfo
from Implementation.SyncResponse import SyncResponse
from Implementation.ClientJsonSerelizationOption import *
from Implementation.libs import tornado
from Implementation.Body.CreateStreamRequestBody import CreateStreamRequestBody
from Implementation.Body.DeleteStreamRequestBody import DeleteStreamRequestBody
from Implementation.Body.AppendToStreamRequestBody import AppendToStreamRequestBody
from Implementation.ReturnClasses.FailedAnswer import FailedAnswer
from Implementation.ReturnClasses.AllEventsAnswer import AllEventsAnswer
from Implementation.libs.tornado import httpclient


class ClientAPI():
  def __init__(self, ip_address="http://127.0.0.1", port = 2113):
    self.base_url = ip_address+":"+str(port)
    self.headers = {"content-type" : "application/json","accept" : "application/json", "extensions" : "json"}
    self.read_batch_size = 20
    self.TornadoHttpSender = TornadoHttpSender()
    self.subscribers = []
    self.subscribers_thread = thread.start_new_thread(self.handle_subscribers, ())
    self.subscribers_all_thread = thread.start_new_thread(self.handle_subscribers_all, ())
    self.should_subscribe_all=False


  def resume(self):
    tornado.ioloop.IOLoop.instance().stop()

  def wait(self):
    tornado.ioloop.IOLoop.instance().start()


  def sync_success(self, response):
    self.resume()
    response = SyncResponse(True, response)
    return response
  def sync_failed(self, response):
    self.resume()
    response = SyncResponse(False, response)
    return response


################################################################################


  def create_stream(self, stream_id, metadata=""):
    queue = deque()
    on_success = lambda x: queue.append(self.sync_success(x))
    on_failed = lambda x: queue.append(self.sync_failed(x))
    self.create_stream_async(stream_id, metadata, on_success, on_failed)
    self.wait()
    result = queue.popleft()
    if result.success:
      return
    else:
      raise result.response

  def create_stream_async(self, stream_id, metadata, on_success, on_failed):
    self.start_create_stream(stream_id, metadata, on_success, on_failed)


  def start_create_stream(self, stream_id, metadata, on_success, on_failed):
    Ensure.is_not_empty_string(stream_id, "stream_id")
    Ensure.is_string(metadata, "metadata")
    Ensure.is_function(on_success, "on_success")
    Ensure.is_function(on_failed, "on_failed")
    body = to_json(CreateStreamRequestBody(stream_id, metadata))
    url = "{0}/streams".format(self.base_url)
    self.TornadoHttpSender.send_async(url, "POST", self.headers, body,\
        lambda x: self.create_stream_callbabk(x, on_success, on_failed))

  def create_stream_callbabk(self, response, on_success,on_failed):
    if response.code==201:
      on_success(response)
    else:
      on_failed(FailedAnswer(response.code,response.error.message))



#################################################################



  def delete_stream(self, stream_id, expected_version=-2):
    queue = deque()
    on_success = lambda x: queue.append(self.sync_success(x))
    on_failed = lambda x: queue.append(self.sync_failed(x))
    self.delete_stream_async(stream_id, on_success, on_failed, expected_version)
    self.wait()
    result = queue.popleft()
    if result.success:
      return
    else:
      raise result.response

  def delete_stream_async(self, stream_id, on_success, on_failed, expected_version=-2):
    self.start_delete_stream(stream_id, on_success, on_failed, expected_version)


  def start_delete_stream(self,stream_id, on_success, on_failed, expected_version):
    Ensure.is_not_empty_string(stream_id, "stream_id")
    Ensure.is_function(on_success, "on_success")
    Ensure.is_function(on_failed, "on_failed")
    Ensure.is_number(expected_version, "expected_version")

    url = "{0}/streams/{1}".format(self.base_url, stream_id)
    body = to_json(DeleteStreamRequestBody(expected_version))
    self.TornadoHttpSender.send_async(url, "DELETE", self.headers, body, \
        lambda x: self.delete_stream_callback(x, on_success, on_failed))

  def delete_stream_callback(self, response, on_success, on_failed):
    if response.code==204:
      on_success(response)
    else:
      on_failed(FailedAnswer(response.code,response.error.message))


#######################################################



  def append_to_stream(self,stream_id, events,expected_version=-2):
    queue = deque()
    on_success = lambda x: queue.append(self.sync_success(x))
    on_failed = lambda x: queue.append(self.sync_failed(x))
    self.append_to_stream_async(stream_id, events, on_success, on_failed, expected_version)
    self.wait()
    result = queue.popleft()
    if result.success:
      return
    else:
      raise result.response

  def append_to_stream_async(self,stream_id, events, on_success, on_failed, expected_version=-2):
    self.start_append_to_stream_async(stream_id, events, on_success, on_failed, expected_version)


  def start_append_to_stream_async(self,stream_id, events, on_success, on_failed, expected_version):
    Ensure.is_not_empty_string(stream_id, "stream_id")
    Ensure.is_function(on_success, "on_success")
    Ensure.is_function(on_failed, "on_failed")
    Ensure.is_number(expected_version, "expected_version")

    if(type(events) is not list):
      events = [events]
    body = to_json(AppendToStreamRequestBody(expected_version, events))
    url = "{0}/streams/{1}".format(self.base_url,stream_id)
    self.TornadoHttpSender.send_async(url,"POST", self.headers, body, \
        lambda x: self.append_to_stream_callback(x, on_success, on_failed))


  def append_to_stream_callback(self, response, on_success,on_failed):
    if response.code==201:
      on_success(response)
    else:
      on_failed(FailedAnswer(response.code,response.error.message))



###########################################################


  def read_event(self,stream_id, event_number, resolve=-2):
    queue = deque()
    on_success = lambda x: queue.append(self.sync_success(x))
    on_failed = lambda x: queue.append(self.sync_failed(x))
    self.read_event_async(stream_id, event_number, on_success, on_failed, resolve)
    self.wait()
    result = queue.popleft()
    if result.success:
      return result.response
    else:
      raise result.response

  def read_event_async(self,stream_id, event_number, on_success, on_failed, resolve=-2):
    self.start_read_event(stream_id, event_number, on_success, on_failed, resolve)


  def start_read_event(self,stream_id, event_number, on_success, on_failed, resolve):
    Ensure.is_not_empty_string(stream_id, "stream_id")
    Ensure.is_function(on_success, "on_success")
    Ensure.is_function(on_failed, "on_failed")
    Ensure.is_not_negative_number(event_number, "event_number")
    resolve = "yes" if resolve else "no"
    url = "{0}/streams/{1}/event/{2}?resolve={3}".format(self.base_url, stream_id, str(event_number), resolve)
    self.TornadoHttpSender.send_async(url, "GET", self.headers, None, \
        lambda x: self.read_event_callback(x, on_success, on_failed))

  def read_event_callback(self, response, on_success, on_failed):
    if response.code!=200:
      on_failed(FailedAnswer(response.code,response.error.message))
      return
    responseContent = response.body
    event = json.loads(responseContent)
    on_success(event)



################################################ Read Stream Events Backward ###############



  def read_stream_events_backward_async(self, stream_id, start_position, count, on_success, on_failed):
    self.start_read_stream_events_backward_async(stream_id, start_position, count, on_success, on_failed)

  def read_stream_events_backward(self, stream_id, start_position, count):
    queue = deque()
    on_success = lambda x: queue.append(self.sync_success(x))
    on_failed = lambda x: queue.append(self.sync_failed(x))
    self.read_stream_events_backward_async(stream_id, start_position, count, on_success, on_failed)
    self.wait()
    result = queue.popleft()
    if result.success:
      return result.response
    else:
      raise result.response


  def start_read_stream_events_backward_async(self, stream_id, start_position, count, on_success, on_failed):
    Ensure.is_not_empty_string(stream_id, "stream_id")
    Ensure.is_function(on_success, "on_success")
    Ensure.is_function(on_failed, "on_failed")
    Ensure.is_positive_number(count, "count")
    Ensure.is_possible_event_position(start_position, "start_position")
    events = []
    batch_counter=0
    params = ReadEventsData
    params.stream_id = stream_id
    params.start_position = start_position
    params.count = count
    params.batch_counter = batch_counter
    params.events = events
    self.read_batch_events_backward(params, on_success, on_failed)

  def read_batch_events_backward(self, params, on_success, on_failed, events_count=None):
    if events_count is not None and events_count < self.read_batch_size:
      on_success(params.events)
      return
    if params.batch_counter < params.count:
      if params.count < params.batch_counter + self.read_batch_size:
        params.batch_langth = params.count - params.batch_counter
      else :
        params.batch_langth = self.read_batch_size
      url = self.base_url + "/streams/{0}/range/{1}/{2}".format(
          params.stream_id,
          str(params.start_position-params.batch_counter),
          str(params.batch_langth))
      params.batch_counter+=self.read_batch_size
      self.TornadoHttpSender.send_async(url, "GET", self.headers, None, \
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
        url = "{0}/streams/{1}".format(self.base_url, params.stream_id)
        self.TornadoHttpSender.send_async(url, "GET", self.headers, None, \
            lambda x: self.on_read_events_first_response_entries_empty(x, params, on_success, on_failed))
        return
      batch_events = []
      for uri in response["entries"]:
        url = uri["links"]
        for ur in url:
          try:
            if ur["type"] == "application/json":
              url = ur["uri"]
              self.TornadoHttpSender.send_async(url, "GET", self.headers, None, \
                  lambda x: self.event_read_callback(x, params,batch_events, on_success, on_failed,len(response["entries"])))
              break
          except:
            continue
    except httpclient.HTTPError, e:
      on_failed(FailedAnswer(response.code, "Error occur while process batch: " + response.error.message))
      return

  def on_read_events_first_response_entries_empty(self, response, params, on_success, on_failed):
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
    self.read_batch_events_backward(params, on_success, on_failed)

  def event_read_callback(self, response, params, batch_events, on_success, on_failed, events_count):
    if response.code !=200:
      on_failed(FailedAnswer(response.code, response.error.message))
      return
    try:
      batch_events.append(json.loads(response.body))
      if len(batch_events)==events_count:
        batch_events = sorted(batch_events, key=lambda ev: ev["eventNumber"], reverse=True)
        params.events+=batch_events
        self.read_batch_events_backward(params, on_success, on_failed, events_count)
    except:
      on_failed(FailedAnswer(response.code, "Error occure while reading event: " + response.error.message))
      return



##########################################



  def read_stream_events_forward_async(self, stream_id, start_position, count, on_success, on_failed):
    self.start_read_stream_events_backward_async(stream_id, start_position+count-1, count, on_success, on_failed)

  def read_stream_events_forward(self, stream_id, start_position, count):
    queue = deque()
    on_success = lambda x: queue.append(self.sync_success(list(reversed(x))))
    on_failed = lambda x: queue.append(self.sync_failed(x))
    self.read_stream_events_forward_async(stream_id, start_position, count, on_success, on_failed)
    self.wait()
    result = queue.popleft()
    if result.success:
      return result.response
    else:
      raise result.response



############################################# READ ALL EVENTS BACKWARD #################################################


  def read_all_events_backward(self, prepare_position, commit_position, count):
    queue = deque()
    on_success = lambda x: queue.append(self.sync_success(x))
    on_failed = lambda x: queue.append(self.sync_failed(x))
    self.start_read_all_events_backward(prepare_position, commit_position, count, on_success, on_failed)
    self.wait()
    result = queue.popleft()
    if result.success:
      return result.response
    else:
      raise result.response


  def read_all_events_backward_async(self, prepare_position, commit_position, count, on_success, on_failed):
    self.start_read_all_events_backward(prepare_position, commit_position, count, on_success, on_failed)


  def start_read_all_events_backward(self, prepare_position, commit_position, count, on_success, on_failed):
    Ensure.is_function(on_success, "on_success")
    Ensure.is_function(on_failed, "on_failed")
    Ensure.is_number(prepare_position, "prepare_position")
    Ensure.is_number(commit_position, "commit_position")
    Ensure.is_positive_number(count, "count")
    events = []
    batch_counter = 0
    params = ReadEventsData
    params.prepare_position = prepare_position
    params.commit_position = commit_position
    params.count = count
    params.batch_counter = batch_counter
    params.events = events
    self.read_batch_all_events_backward(params, on_success, on_failed)

  def read_batch_all_events_backward(self, params, on_success, on_failed, events_count=None):
    if events_count is not None and events_count < self.read_batch_size:
      on_success(AllEventsAnswer(params.events, params.prepare_position, params.commit_position))
      return
    if params.batch_counter < params.count:
      hexStartPosition = self.convert_to_hex(params.prepare_position) + self.convert_to_hex(params.commit_position)
      if params.count < params.batch_counter + self.read_batch_size:
        params.batch_langth = params.count - params.batch_counter
      else :
        params.batch_langth = self.read_batch_size
      url = "{0}/streams/$all/before/{1}/{2}".format(self.base_url, hexStartPosition, str(params.batch_langth))
      params.batch_counter+=self.read_batch_size
      self.TornadoHttpSender.send_async(url, "GET", self.headers, None, \
          lambda x: self.read_all_events_backward_page_callback(x, params, on_success, on_failed))
    else:
      on_success(AllEventsAnswer(params.events, params.prepare_position, params.commit_position))

  def read_all_events_backward_page_callback(self, response, params, on_success, on_failed):
    if response.code!=200:
      on_failed(FailedAnswer(response.code,"Error occur while reading links: " + response.error.message))
    read_line = response.body
    body = json.loads(read_line)
    params.prepare_position = self.get_prepare_position(body["links"][4]["uri"])
    params.commit_position = self.get_commit_position(body["links"][4]["uri"])

    try:
      if body["entries"] == []:
        if len(params.events)!=0:
          on_success(AllEventsAnswer("", 0,0 ))
        else:
          self.start_read_all_events_backward(-1,-1, params.count, on_success, on_failed)
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
              self.TornadoHttpSender.send_async(url, "GET", self.headers, None, \
                  lambda x: self.read_all_events_backward_callback(x, params, batch_events, on_success, on_failed,events_count, url_number_dictionary))
              break
          except:
            continue
    except:
      on_failed(FailedAnswer(response.code,response.error.message))
      return

  def read_all_events_backward_callback(self, response, params, batch_events, on_success, on_failed, events_count, url_number_dictionary):
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
        self.read_batch_all_events_backward(params, on_success, on_failed, events_count)
    except:
      on_failed(FailedAnswer(response.code,"Error occure while reading event: "+response.error.message))



############################################# READ ALL EVENTS FORWARD ##################################################


  def read_all_events_forward(self, prepare_position, commit_position, count):
    queue = deque()
    on_success = lambda x: queue.append(self.sync_success(x))
    on_failed = lambda x: queue.append(self.sync_failed(x))
    self.read_all_events_forward_async(prepare_position, commit_position, count, on_success, on_failed)
    self.wait()
    result = queue.popleft()
    if result.success:
      return result.response
    else:
      raise result.response


  def read_all_events_forward_async(self, prepare_position, commit_position, count, on_success, on_failed):
    self.start_read_all_events_forward(prepare_position, commit_position, count, on_success, on_failed)


  def start_read_all_events_forward(self, prepare_position, commit_position, count, on_success, on_failed):
    Ensure.is_function(on_success, "on_success")
    Ensure.is_function(on_failed, "on_failed")
    Ensure.is_possible_event_position(prepare_position, "prepare_position")
    Ensure.is_possible_event_position(commit_position, "commit_position")
    Ensure.is_positive_number(count, "count")
    events = []
    batch_counter = 0
    params = ReadEventsData
    params.prepare_position = prepare_position
    params.commit_position = commit_position
    params.count = count
    params.batch_counter = batch_counter
    params.events = events
    self.read_batch_all_events_forward(params, on_success, on_failed)

  def read_batch_all_events_forward(self, params, on_success, on_failed, events_count=None):
    if events_count!=None and events_count<self.read_batch_size:
      on_success(AllEventsAnswer(params.events, params.prepare_position, params.commit_position))
      return
    if params.batch_counter<params.count:
      hexStartPosition = self.convert_to_hex(params.prepare_position) + self.convert_to_hex(params.commit_position)
      if params.batch_counter+self.read_batch_size>params.count:
        params.batch_langth = params.count - params.batch_counter
      else :
        params.batch_langth = self.read_batch_size
      url = "{0}/streams/$all/after/{1}/{2}".format(self.base_url, hexStartPosition, str(params.batch_langth))
      params.batch_counter+=self.read_batch_size
      self.TornadoHttpSender.send_async(url, "GET", self.headers, None, lambda x: \
      self.read_all_events_forward_page_callback(x, params, on_success, on_failed))
    else:
      on_success(AllEventsAnswer(params.events, params.prepare_position, params.commit_position))

  def read_all_events_forward_page_callback(self, response, params, on_success, on_failed):
    if response.code!=200:
      on_failed(FailedAnswer(response.code,"Error occur while reading links: " + response.error.message))
    read_line = response.body
    body = json.loads(read_line)
    params.prepare_position = self.get_prepare_position(body["links"][3]["uri"])
    params.commit_position = self.get_commit_position(body["links"][3]["uri"])
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
              self.TornadoHttpSender.send_async(url, "GET", self.headers, None,\
                  lambda x: self.read_all_events_forward_callback(x, params, batch_events, on_success, on_failed,events_count, url_number_dictionary))
              break
          except:
            continue
    except:
      on_failed(FailedAnswer(response.code,response.error.message))
      return

  def read_all_events_forward_callback(self, response, params, batch_events, on_success, on_failed,events_count, url_number_dictionary):
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
        self.read_batch_all_events_forward(params, on_success, on_failed, events_count)
    except:
      on_failed(FailedAnswer(response.code, "Error occure while reading event: " + response.error.message))




#################################################################################################################

  def get_prepare_position(self, link):
    position = link.split("/")[6]
    result =int(position[0:16], 16)
    return result

  def get_commit_position(self, link):
    position = link.split("/")[6]
    result =int(position[16:32], 16)
    return result

  def convert_to_hex(self, number):
    if number<0:
      return"ffffffffffffffff"
    hexVal = hex(number)
    number = str(hexVal).split("x")[1]
    while len(number)!=16:
      number = "0"+number
    return number


###############################################################################################################


  def subscribe(self, stream_id, callback, start_from_begining = False):
    start_position = 0 if start_from_begining else self.get_stream_event_position(stream_id) + 1
    self.subscribers.append(SubscribeInfo(stream_id,start_position, callback))

  def handle_subscribers(self):
    while True:
      if len(self.subscribers)>0:
        processed_streams=0
        for i in range(len(self.subscribers)):
          self.read_stream_events_forward_async(self.subscribers[i].stream_id, self.subscribers[i].last_position, 100,\
              lambda s: self.handle_subscribers_success(s, len(self.subscribers), processed_streams, i),\
              lambda s: self.handle_subscribers_success(s, len(self.subscribers), processed_streams, i))
        self.wait()
      time.sleep(1)

  def handle_subscribers_success(self, response, stream_count, processed_streams, subscriber_index):
    self.subscribers[subscriber_index].last_position += len(response)
    processed_streams+=1

    for i in range(len(response)):
      self.subscribers[subscriber_index].callback(response[i])

    if processed_streams==stream_count:
      self.resume()

  def unsubscribe(self, stream_id):
    for i in range(len(self.subscribers)):
      self.subscribers.remove(self.subscribers[i])



####################################################################################################################


  def subscribe_all(self, callback, start_from_begining = False):
    start_position = 0 if start_from_begining else -1

    self.__subscribersAll=SubscribeAllInfo(start_position, start_position, callback)
    self.should_subscribe_all=True

  def handle_subscribers_all(self):
    while True:
      if self.should_subscribe_all:
        if self.__subscribersAll.commit_position ==-1:
          answer = self.read_all_events_backward(self.__subscribersAll.prepare_position,self.__subscribersAll.commit_position, 1)
        else:
          try:
            answer = self.read_all_events_forward(self.__subscribersAll.prepare_position,self.__subscribersAll.commit_position, sys.maxint-1)
            for i in range(len(answer.events)):
              self.__subscribersAll.callback(answer.events[i])
          except:
            time.sleep(0.1)


        self.__subscribersAll.commit_position = int(answer.commit_position)
        self.__subscribersAll.prepare_position = int(answer.prepare_position)

      time.sleep(1)

  def unsubscribe_all(self):
    self.should_subscribe_all = False


#######################################################################################################################


  def get_stream_event_position(self,stream_id):
    events = self.read_stream_events_backward(stream_id, -1, 1)
    return events[0]["eventNumber"]


#######################################################################################################################


  class Projections:

    def enable(self, name):
      queue = deque()
      on_success = lambda x: queue.append(ClientAPI.__SyncSuccess(x))
      on_failed = lambda x: queue.append(ClientAPI.__SyncFailed(x))
      self.start_enable_async(name, on_success, on_failed)
      ClientAPI.wait()
      result = queue.popleft()
      if result.success:
        return result.response
      else:
        raise result.response

    def start_enable_async(self,name, on_success, on_failed):
      Ensure.is_not_empty_string(name, "name")
      Ensure.is_function(on_success, "on_success")
      Ensure.is_function(on_failed, "on_failed")

      url = "{0}/projection/{1}/command/enable".format(ClientAPI.base_url, name)
      ClientAPI.__tornadoHttpSender.send_async(url, "POST", ClientAPI.headers, None, \
          lambda s: self.on_enable(s, on_success, on_failed))

    def on_enable(self, response, on_success, on_failed):
      if response.status == "Ok":
        on_success(response)
        return
      on_failed(response)




#######################################################################################################################

    def disable(self, name):
      queue = deque()
      on_success = lambda x: queue.append(ClientAPI.__SyncSuccess(x))
      on_failed = lambda x: queue.append(ClientAPI.__SyncFailed(x))
      self.start_disable_async(name, on_success, on_failed)
      ClientAPI.wait()
      result = queue.popleft()
      if result.success:
        return result.response
      else:
        raise result.response

    def start_disable_async(self, name, on_success, on_failed):
      Ensure.is_not_empty_string(name, "name")
      Ensure.is_function(on_success, "on_success")
      Ensure.is_function(on_failed, "on_failed")

      url = "{0}/projection/{1}/command/disable".format(ClientAPI.base_url, name)
      ClientAPI.__tornadoHttpSender.send_async(url, "POST", ClientAPI.headers, None, \
          lambda s: self.on_disable(s, on_success, on_failed))

    def on_disable(self, response, on_success, on_failed):
      if response.status == "Ok":
        on_success(response)
        return
      on_failed(response)


#######################################################################################################################

    def create_one_time(self,query):
      queue = deque()
      on_success = lambda x: queue.append(ClientAPI.__SyncSuccess(x))
      on_failed = lambda x: queue.append(ClientAPI.__SyncFailed(x))
      self.start_create_one_time_async(query, on_success, on_failed)
      ClientAPI.wait()
      result = queue.popleft()
      if result.success:
        return result.response
      else:
        raise result.response

    def start_create_one_time_async(self, query, on_success, on_failed):
      Ensure.is_not_empty_string(query, "query")
      Ensure.is_function(on_success, "on_success")
      Ensure.is_function(on_failed, "on_failed")

      url = "{0}/projections/onetime?type=JS".format(ClientAPI.base_url)
      ClientAPI.__tornadoHttpSender.send_async(url, "POST", ClientAPI.headers, query, \
          lambda s: self.on_create_one_time(s, on_success, on_failed))

    def on_create_one_time(self, response, on_success, on_failed):
      if response.status == "Created":
        on_success(response)
        return
      on_failed(response)


#######################################################################################################################

    def create_continuous(self, name, query):
      queue = deque()
      on_success = lambda x: queue.append(ClientAPI.__SyncSuccess(x))
      on_failed = lambda x: queue.append(ClientAPI.__SyncFailed(x))
      self.start_create_continuous_async(name, query, on_success, on_failed)
      ClientAPI.wait()
      result = queue.popleft()
      if result.success:
        return result.response
      else:
        raise result.response

    def start_create_continuous_async(self,name, query, on_success, on_failed):
      Ensure.is_not_empty_string(name, "name")
      Ensure.is_not_empty_string(query, "query")
      Ensure.is_function(on_success, "on_success")
      Ensure.is_function(on_failed, "on_failed")

      url = "{0}/projections/continuous?name={1}&type=JS".format(ClientAPI.base_url, name)
      ClientAPI.__tornadoHttpSender.send_async(url, "POST", ClientAPI.headers, query, \
          lambda s: self.on_create_one_time(s, on_success, on_failed))

    def on_create_continuous(self, response, on_success, on_failed):
      if response.status == "Created":
        on_success(response)
        return
      on_failed(response)


#######################################################################################################################

    def update_query(self, name, query):
      queue = deque()
      on_success = lambda x: queue.append(ClientAPI.__SyncSuccess(x))
      on_failed = lambda x: queue.append(ClientAPI.__SyncFailed(x))
      self.start_update_query_async(name, query, on_success, on_failed)
      ClientAPI.wait()
      result = queue.popleft()
      if result.success:
        return result.response
      else:
        raise result.response
    def start_update_query_async(self,name, query, on_success, on_failed):
      Ensure.is_not_empty_string(name, "name")
      Ensure.is_not_empty_string(query, "query")
      Ensure.is_function(on_success, "on_success")
      Ensure.is_function(on_failed, "on_failed")

      url = "{0}/projection/{1}/query?type=JS".format(ClientAPI.base_url, name)
      ClientAPI.__tornadoHttpSender.send_async(url, "POST", ClientAPI.headers, query, \
          lambda s: self.on_update_query(s, on_success, on_failed))

    def on_update_query(self, response, on_success, on_failed):
      if response.status == "Ok":
        on_success(response)
        return
      on_failed(response)


#######################################################################################################################


    def delete(self, name):
      queue = deque()
      on_success = lambda x: queue.append(ClientAPI.__SyncSuccess(x))
      on_failed = lambda x: queue.append(ClientAPI.__SyncFailed(x))
      self.start_delete_async(name, on_success, on_failed)
      ClientAPI.wait()
      result = queue.popleft()
      if result.success:
        return result.response
      else:
        raise result.response

    def start_delete_async(self, name, on_success, on_failed):
      Ensure.is_not_empty_string(name, "name")
      Ensure.is_function(on_success, "on_success")
      Ensure.is_function(on_failed, "on_failed")

      url = "{0}/projection/{1}".format(ClientAPI.base_url, name)
      ClientAPI.__tornadoHttpSender.send_async(url, "DELETE", ClientAPI.headers, None, \
          lambda s: self.on_delete(s, on_success, on_failed))

    def on_delete(self, response, on_success, on_failed):
      if response.status == "Ok":
        on_success(response)
        return
      on_failed(response)


#######################################################################################################################

    def list_all(self):
      queue = deque()
      on_success = lambda x: queue.append(ClientAPI.__SyncSuccess(x))
      on_failed = lambda x: queue.append(ClientAPI.__SyncFailed(x))
      self.start_list_contionuous_async(on_success, on_failed)
      ClientAPI.wait()
      result = queue.popleft()
      if result.success:
        return result.response
      else:
        raise result.response

    def start_list_all_async(self, on_success, on_failed):
      Ensure.is_function(on_success, "on_success")
      Ensure.is_function(on_failed, "on_failed")

      url = "{0}/projections/any".format(ClientAPI.base_url)
      ClientAPI.__tornadoHttpSender.send_async(url, "GET", ClientAPI.headers, None, \
          lambda s: self.on_list_all(s, on_success, on_failed))

    def on_list_all(self, response, on_success, on_failed):
      if response.status == "Ok":
        on_success(response.message)
        return
      on_failed(response)


######################################################################################################################

    def list_one_time(self):
      queue = deque()
      on_success = lambda x: queue.append(ClientAPI.__SyncSuccess(x))
      on_failed = lambda x: queue.append(ClientAPI.__SyncFailed(x))
      self.start_list_one_time_async(on_success, on_failed)
      ClientAPI.wait()
      result = queue.popleft()
      if result.success:
        return result.response
      else:
        raise result.response

    def start_list_one_time_async(self, on_success, on_failed):
      Ensure.is_function(on_success, "on_success")
      Ensure.is_function(on_failed, "on_failed")

      url = "{0}/projections/onetime".format(ClientAPI.base_url)
      ClientAPI.__tornadoHttpSender.send_async(url, "GET", ClientAPI.headers, None, \
          lambda s: self.on_list_one_time(s, on_success, on_failed))

    def on_list_one_time(self, response, on_success, on_failed):
      if response.status == "Ok":
        on_success(response.message)
        return
      on_failed(response)


######################################################################################################################

    def list_continuous(self):
      queue = deque()
      on_success = lambda x: queue.append(ClientAPI.__SyncSuccess(x))
      on_failed = lambda x: queue.append(ClientAPI.__SyncFailed(x))
      self.start_list_contionuous_async(on_success, on_failed)
      ClientAPI.wait()
      result = queue.popleft()
      if result.success:
        return result.response
      else:
        raise result.response

    def start_list_contionuous_async(self, on_success, on_failed):
      Ensure.is_function(on_success, "on_success")
      Ensure.is_function(on_failed, "on_failed")

      url = "{0}/projections/continuous".format(ClientAPI.base_url)
      ClientAPI.__tornadoHttpSender.send_async(url, "GET", ClientAPI.headers, None, \
          lambda s: self.on_list_continuous(s, on_success, on_failed))

    def on_list_continuous(self, response, on_success, on_failed):
      if response.status == "Ok":
        on_success(response)
        return
      on_failed(response)


########################################################################################################################