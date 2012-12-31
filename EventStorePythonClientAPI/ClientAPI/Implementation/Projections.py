from collections import deque
import json
from tornado.ioloop import IOLoop
import Ensure
from SyncResponse import *
from AsyncRequestSender import TornadoHttpSender

class Projections:
    def __init__(self, ip_address="127.0.0.1", port = 2113):
        self._base_url = "http://"+ip_address+":"+str(port)
        self._headers = {"content-type" : "application/json", "accept" : "application/json", "extensions" : "json"}
        self._tornado_http_sender = TornadoHttpSender()


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



    def enable(self, name):
        queue = deque()
        on_success = lambda x: queue.append(self._sync_success(x))
        on_failed = lambda x: queue.append(self._sync_failed(x))
        self._start_enable_async(name, on_success, on_failed)
        self.wait()
        result = queue.popleft()
        if result.success:
            return result.response
        else:
            raise result.response

    def _start_enable_async(self,name, on_success, on_failed):
        Ensure.is_not_empty_string(name, "name")
        Ensure.is_function(on_success, "on_success")
        Ensure.is_function(on_failed, "on_failed")

        url = "{0}/projection/{1}/command/enable".format(self._base_url, name)
        self._tornado_http_sender.send_async(url, "POST", self._headers, None,
            lambda s: self._on_enable(s, on_success, on_failed))

    def _on_enable(self, response, on_success, on_failed):
        if response.code == 201:
            on_success(response)
            return
        on_failed(response)



    def disable(self, name):
        queue = deque()
        on_success = lambda x: queue.append(self._sync_success(x))
        on_failed = lambda x: queue.append(self._sync_failed(x))
        self._start_disable_async(name, on_success, on_failed)
        self.wait()
        result = queue.popleft()
        if result.success:
            return result.response
        else:
            raise result.response

    def _start_disable_async(self, name, on_success, on_failed):
        Ensure.is_not_empty_string(name, "name")
        Ensure.is_function(on_success, "on_success")
        Ensure.is_function(on_failed, "on_failed")

        url = "{0}/projection/{1}/command/disable".format(self._base_url, name)
        self._tornado_http_sender.send_async(url, "POST", self._headers, None,
            lambda s: self._on_disable(s, on_success, on_failed))

    def _on_disable(self, response, on_success, on_failed):
        if response.code == 201:
            on_success(response)
            return
        on_failed(response)



    def get_projections(self):
        queue = deque()
        on_success = lambda x: queue.append(self._sync_success(x))
        on_failed = lambda x: queue.append(self._sync_failed(x))
        self._start_get_projections_async(on_success, on_failed)
        self.wait()
        result = queue.popleft()
        if result.success:
            return result.response
        else:
            raise result.response

    def _start_get_projections_async(self, on_success, on_failed):
        Ensure.is_function(on_success, "on_success")
        Ensure.is_function(on_failed, "on_failed")

        url = "{0}/projections".format(self._base_url)
        self._tornado_http_sender.send_async(url, "GET", self._headers, None,
            lambda s: self._on_get_projections(s, on_success, on_failed))

    def _on_get_projections(self, response, on_success, on_failed):
        if response.code == 200:
            on_success(json.loads(response.message))
            return
        on_failed(response)



    def get_any(self):
        queue = deque()
        on_success = lambda x: queue.append(self._sync_success(x))
        on_failed = lambda x: queue.append(self._sync_failed(x))
        self._start_get_any_async(on_success, on_failed)
        self.wait()
        result = queue.popleft()
        if result.success:
            return result.response
        else:
            raise result.response

    def _start_get_any_async(self, on_success, on_failed):
        Ensure.is_function(on_success, "on_success")
        Ensure.is_function(on_failed, "on_failed")

        url = "{0}/projections/any".format(self._base_url)
        self._tornado_http_sender.send_async(url, "GET", self._headers, None,
            lambda s: self._on_get_any(s, on_success, on_failed))

    def _on_get_any(self, response, on_success, on_failed):
        if response.code == 200:
            on_success(json.loads(response.message))
            return
        on_failed(response)



    def get_one_time(self):
        queue = deque()
        on_success = lambda x: queue.append(self._sync_success(x))
        on_failed = lambda x: queue.append(self._sync_failed(x))
        self._start_get_one_time_async(on_success, on_failed)
        self.wait()
        result = queue.popleft()
        if result.success:
            return result.response
        else:
            raise result.response

    def _start_get_one_time_async(self, on_success, on_failed):
        Ensure.is_function(on_success, "on_success")
        Ensure.is_function(on_failed, "on_failed")

        url = "{0}/projections/onetime".format(self._base_url)
        self._tornado_http_sender.send_async(url, "GET", self._headers, None,
            lambda s: self._on_get_one_time(s, on_success, on_failed))

    def _on_get_one_time(self, response, on_success, on_failed):
        if response.code == 200:
            on_success(response.message)
            return
        on_failed(response)



    def post_one_time(self, query, name=None, type=None, enabled=None, emit=None):
        queue = deque()
        on_success = lambda x: queue.append(self._sync_success(x))
        on_failed = lambda x: queue.append(self._sync_failed(x))
        self._start_post_one_time_async(name, query, type, enabled, emit, on_success, on_failed)
        self.wait()
        result = queue.popleft()
        if result.success:
            return result.response
        else:
            raise result.response

    def _start_post_one_time_async(self, name, query, type, enabled, emit, on_success, on_failed):
        Ensure.is_function(on_success, "on_success")
        Ensure.is_function(on_failed, "on_failed")

        url = Url("{0}/projections/onetime".format(self._base_url)).param("name", name).param("type", type).param("enabled", enabled).param("emit", emit).get_url()
        self._tornado_http_sender.send_async(url, "POST", self._headers, query,
            lambda s: self._on_post_one_time(s, on_success, on_failed))

    def _on_post_one_time(self, response, on_success, on_failed):
        if response.code == 201:
            on_success(response.message)
            return
        on_failed(response)



    def get_continuous(self):
        queue = deque()
        on_success = lambda x: queue.append(self._sync_success(x))
        on_failed = lambda x: queue.append(self._sync_failed(x))
        self._start_get_contionuous_async(on_success, on_failed)
        self.wait()
        result = queue.popleft()
        if result.success:
            return result.response
        else:
            raise result.response

    def _start_get_contionuous_async(self, on_success, on_failed):
        Ensure.is_function(on_success, "on_success")
        Ensure.is_function(on_failed, "on_failed")

        url = "{0}/projections/continuous".format(self._base_url)
        self._tornado_http_sender.send_async(url, "GET", self._headers, None,
            lambda s: self._on_get_continuous(s, on_success, on_failed))

    def _on_get_continuous(self, response, on_success, on_failed):
        if response.code == 200:
            on_success(response)
            return
        on_failed(response)



    def post_continuous(self, query, name=None, type=None, enabled=None, emit=None):
        queue = deque()
        on_success = lambda x: queue.append(self._sync_success(x))
        on_failed = lambda x: queue.append(self._sync_failed(x))
        self._start_post_contionuous_async(name, query, type, enabled, emit, on_success, on_failed)
        self.wait()
        result = queue.popleft()
        if result.success:
            return result.response
        else:
            raise result.response

    def _start_post_contionuous_async(self, name, query, type, enabled, emit, on_success, on_failed):
        Ensure.is_function(on_success, "on_success")
        Ensure.is_function(on_failed, "on_failed")

        url = Url("{0}/projections/continuous".format(self._base_url)).param("name", name).param("type", type).param("enabled", enabled).param("emit", emit).get_url()
        self._tornado_http_sender.send_async(url, "POST", None, query,
            lambda s: self._on_post_continuous(s, on_success, on_failed))

    def _on_post_continuous(self, response, on_success, on_failed):
        if response.code == 201:
            on_success(response)
            return
        on_failed(response)



    def query_get(self, name = None, config = True):
        queue = deque()
        on_success = lambda x: queue.append(self._sync_success(x))
        on_failed = lambda x: queue.append(self._sync_failed(x))
        self._start_query_get_async(name, config, on_success, on_failed)
        self.wait()
        result = queue.popleft()
        if result.success:
            return result.response
        else:
            raise result.response

    def _start_query_get_async(self, name, config, on_success, on_failed):
        Ensure.is_function(on_success, "on_success")
        Ensure.is_function(on_failed, "on_failed")
        Ensure.is_string(name, "name")

        url = Url("{0}/projection/{1}/query".format(self._base_url, name)).param("config", config).get_url()
        self._tornado_http_sender.send_async(url, "GET", self._headers, None,
            lambda s: self._on_query_get(s, on_success, on_failed))

    def _on_query_get(self, response, on_success, on_failed):
        if response.code == 200:
            on_success(json.loads(response))
            return
        on_failed(response)



    def query_put(self, name, query, type = None, emit = None):
        queue = deque()
        on_success = lambda x: queue.append(self._sync_success(x))
        on_failed = lambda x: queue.append(self._sync_failed(x))
        self._start_query_put_async(name, query, type, emit, on_success, on_failed)
        self.wait()
        result = queue.popleft()
        if result.success:
            return result.response
        else:
            raise result.response

    def _start_query_put_async(self,name, query, type, emit, on_success, on_failed):
        Ensure.is_not_empty_string(name, "name")
        Ensure.is_not_empty_string(query, "query")
        Ensure.is_function(on_success, "on_success")
        Ensure.is_function(on_failed, "on_failed")

        url = Url("{0}/projection/{1}/query".format(self._base_url, name)).param("type", type).param("emit", emit).get_url()
        self._tornado_http_sender.send_async(url, "POST", self._headers, query,
            lambda s: self._on_query_put(s, on_success, on_failed))

    def _on_query_put(self, response, on_success, on_failed):
        if response.code == 201:
            on_success(response)
            return
        on_failed(response)



    def status_get(self, name):
        queue = deque()
        on_success = lambda x: queue.append(self._sync_success(x))
        on_failed = lambda x: queue.append(self._sync_failed(x))
        self._start_status_get_async(name, on_success, on_failed)
        self.wait()
        result = queue.popleft()
        if result.success:
            return result.response
        else:
            raise result.response

    def _start_status_get_async(self, name, on_success, on_failed):
        Ensure.is_function(on_success, "on_success")
        Ensure.is_function(on_failed, "on_failed")
        Ensure.is_string(name, "name")

        url = "{0}/projection/{1}".format(self._base_url, name)
        self._tornado_http_sender.send_async(url, "GET", self._headers, None,
            lambda s: self._on_status_get(s, on_success, on_failed))

    def _on_status_get(self, response, on_success, on_failed):
        if response.code == 200:
            on_success(response)
            return
        on_failed(response)



    def delete(self, name, deleteStateStream=None, deleteCheckpointStream=None):
        queue = deque()
        on_success = lambda x: queue.append(self._sync_success(x))
        on_failed = lambda x: queue.append(self._sync_failed(x))
        self._start_delete_async(name,deleteStateStream, deleteCheckpointStream, on_success, on_failed)
        self.wait()
        result = queue.popleft()
        if result.success:
            return result.response
        else:
            raise result.response

    def _start_delete_async(self, name, deleteStateStream, deleteCheckpointStream, on_success, on_failed):
        Ensure.is_not_empty_string(name, "name")
        Ensure.is_function(on_success, "on_success")
        Ensure.is_function(on_failed, "on_failed")

        url = Url("{0}/projection/{1}".format(self._base_url, name)).param("deleteStateStream", deleteStateStream).param("deleteCheckpointStream", deleteCheckpointStream).get_url()
        self._tornado_http_sender.send_async(url, "DELETE", self._headers, None,
            lambda s: self._on_delete(s, on_success, on_failed))

    def _on_delete(self, response, on_success, on_failed):
        if response.code == 204:
            on_success(response)
            return
        on_failed(response)



    def statistics_get(self, name):
        queue = deque()
        on_success = lambda x: queue.append(self._sync_success(x))
        on_failed = lambda x: queue.append(self._sync_failed(x))
        self._start_statistics_get_async(name, on_success, on_failed)
        self.wait()
        result = queue.popleft()
        if result.success:
            return result.response
        else:
            raise result.response

    def _start_statistics_get_async(self, name, on_success, on_failed):
        Ensure.is_function(on_success, "on_success")
        Ensure.is_function(on_failed, "on_failed")
        Ensure.is_string(name, "name")

        url = "{0}/projection/{1}/statistics".format(self._base_url, name)
        self._tornado_http_sender.send_async(url, "GET", self._headers, None,
            lambda s: self._on_statistics_get(s, on_success, on_failed))

    def _on_statistics_get(self, response, on_success, on_failed):
        if response.code == 200:
            on_success(response)
            return
        on_failed(response)



    def state_get(self, name, partition=None):
        queue = deque()
        on_success = lambda x: queue.append(self._sync_success(x))
        on_failed = lambda x: queue.append(self._sync_failed(x))
        self._start_state_get_async(name, partition, on_success, on_failed)
        self.wait()
        result = queue.popleft()
        if result.success:
            return result.response
        else:
            raise result.response

    def _start_state_get_async(self, name, partition, on_success, on_failed):
        Ensure.is_function(on_success, "on_success")
        Ensure.is_function(on_failed, "on_failed")
        Ensure.is_string(name, "name")

        url = Url("{0}/projection/{1}/state".format(self._base_url, name)).param("partition", partition).get_url()
        self._tornado_http_sender.send_async(url, "GET", self._headers, None,
            lambda s: self._on_state_get(s, on_success, on_failed))

    def _on_state_get(self, response, on_success, on_failed):
        if response.code == 200:
            on_success(response)
            return
        on_failed(response)



class Url:
    def __init__(self, baseUrl):
        self._url = baseUrl
        self._has_params = False

    def param(self, name, value):
        if value is  not None:
            if not self._has_params:
                self._url+="?{0}={1}".format(name, str(value))
                self._has_params = True
            else:
                self._url+="&{0}={1}".format(name, str(value))
        return self

    def get_url(self):
        return self._url


########################################################################################################################