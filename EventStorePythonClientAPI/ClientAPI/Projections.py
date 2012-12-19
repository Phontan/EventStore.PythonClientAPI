from ClientAPI import *


class Projections:
    def __init__(self, ip_address="http://127.0.0.1", port = 2113):
        self.base_url = ip_address+":"+str(port)
        self.headers = {"content-type" : "application/json", "accept" : "application/json", "extensions" : "json"}
        self.read_batch_size = 20
        self.tornado_http_sender = TornadoHttpSender()


    def resume(self):
        IOLoop.instance().stop()
    def wait(self):
        IOLoop.instance().start()

    def sync_success(self, response):
        self.resume()
        response = SyncResponse(True, response)
        return response
    def sync_failed(self, response):
        self.resume()
        response = SyncResponse(False, response)
        return response


########################################################################################################################

    def enable(self, name):
        queue = deque()
        on_success = lambda x: queue.append(self.sync_success(x))
        on_failed = lambda x: queue.append(self.sync_failed(x))
        self.start_enable_async(name, on_success, on_failed)
        self.wait()
        result = queue.popleft()
        if result.success:
            return result.response
        else:
            raise result.response

    def start_enable_async(self,name, on_success, on_failed):
        Ensure.is_not_empty_string(name, "name")
        Ensure.is_function(on_success, "on_success")
        Ensure.is_function(on_failed, "on_failed")

        url = "{0}/projection/{1}/command/enable".format(self.base_url, name)
        self.tornado_http_sender.send_async(url, "POST", self.headers, None,
            lambda s: self.on_enable(s, on_success, on_failed))

    def on_enable(self, response, on_success, on_failed):
        if response.status == "OK":
            on_success(response)
            return
        on_failed(response)


#######################################################################################################################


    def disable(self, name):
        queue = deque()
        on_success = lambda x: queue.append(self.sync_success(x))
        on_failed = lambda x: queue.append(self.sync_failed(x))
        self.start_disable_async(name, on_success, on_failed)
        self.wait()
        result = queue.popleft()
        if result.success:
            return result.response
        else:
            raise result.response

    def start_disable_async(self, name, on_success, on_failed):
        Ensure.is_not_empty_string(name, "name")
        Ensure.is_function(on_success, "on_success")
        Ensure.is_function(on_failed, "on_failed")

        url = "{0}/projection/{1}/command/disable".format(self.base_url, name)
        self.tornado_http_sender.send_async(url, "POST", self.headers, None,
            lambda s: self.on_disable(s, on_success, on_failed))

    def on_disable(self, response, on_success, on_failed):
        if response.status == "OK":
            on_success(response)
            return
        on_failed(response)


    #######################################################################################################################

########################################################################################################################



    def projections(self):
        queue = deque()
        on_success = lambda x: queue.append(self.sync_success(x))
        on_failed = lambda x: queue.append(self.sync_failed(x))
        self.start_projections_async(on_success, on_failed)
        self.wait()
        result = queue.popleft()
        if result.success:
            return result.response
        else:
            raise result.response

    def start_projections_async(self, on_success, on_failed):
        Ensure.is_function(on_success, "on_success")
        Ensure.is_function(on_failed, "on_failed")

        url = "{0}/projections".format(self.base_url)
        self.tornado_http_sender.send_async(url, "GET", self.headers, None,
            lambda s: self.on_projections(s, on_success, on_failed))

    def on_projections(self, response, on_success, on_failed):
        if response.status == "OK":
            on_success(json.loads(response.message))
            return
        on_failed(response)



#######################################################################################################################

    def get_any(self):
        queue = deque()
        on_success = lambda x: queue.append(self.sync_success(x))
        on_failed = lambda x: queue.append(self.sync_failed(x))
        self.start_get_any_async(on_success, on_failed)
        self.wait()
        result = queue.popleft()
        if result.success:
            return result.response
        else:
            raise result.response

    def start_get_any_async(self, on_success, on_failed):
        Ensure.is_function(on_success, "on_success")
        Ensure.is_function(on_failed, "on_failed")

        url = "{0}/projections/any".format(self.base_url)
        self.tornado_http_sender.send_async(url, "GET", self.headers, None,
            lambda s: self.on_get_any(s, on_success, on_failed))

    def on_get_any(self, response, on_success, on_failed):
        if response.status == "OK":
            on_success(json.loads(response.message))
            return
        on_failed(response)


#######################################################################################################################

    def get_one_time(self):
        queue = deque()
        on_success = lambda x: queue.append(self.sync_success(x))
        on_failed = lambda x: queue.append(self.sync_failed(x))
        self.start_get_one_time_async(on_success, on_failed)
        self.wait()
        result = queue.popleft()
        if result.success:
            return result.response
        else:
            raise result.response

    def start_get_one_time_async(self, on_success, on_failed):
        Ensure.is_function(on_success, "on_success")
        Ensure.is_function(on_failed, "on_failed")

        url = "{0}/projections/onetime".format(self.base_url)
        self.tornado_http_sender.send_async(url, "GET", self.headers, None,
            lambda s: self.on_get_one_time(s, on_success, on_failed))

    def on_get_one_time(self, response, on_success, on_failed):
        if response.status == "OK":
            on_success(response.message)
            return
        on_failed(response)


########################################################################################################################


    def post_one_time(self, query, name=None, type=None, enabled=None, emit=None):
        queue = deque()
        on_success = lambda x: queue.append(self.sync_success(x))
        on_failed = lambda x: queue.append(self.sync_failed(x))
        self.start_post_one_time_async(name, query, type, enabled, emit, on_success, on_failed)
        self.wait()
        result = queue.popleft()
        if result.success:
            return result.response
        else:
            raise result.response

    def start_post_one_time_async(self, name, query, type, enabled, emit, on_success, on_failed):
        Ensure.is_function(on_success, "on_success")
        Ensure.is_function(on_failed, "on_failed")

        url = Url("{0}/projections/onetime".format(self.base_url)).param("name", name).param("type", type).param("enabled", enabled).param("emit", emit).get_url()
        self.tornado_http_sender.send_async(url, "POST", self.headers, query,
            lambda s: self.on_post_one_time(s, on_success, on_failed))

    def on_post_one_time(self, response, on_success, on_failed):
        if response.status == "Created":
            on_success(response.message)
            return
        on_failed(response)


######################################################################################################################


    def get_continuous(self):
        queue = deque()
        on_success = lambda x: queue.append(self.sync_success(x))
        on_failed = lambda x: queue.append(self.sync_failed(x))
        self.start_get_contionuous_async(on_success, on_failed)
        self.wait()
        result = queue.popleft()
        if result.success:
            return result.response
        else:
            raise result.response

    def start_get_contionuous_async(self, on_success, on_failed):
        Ensure.is_function(on_success, "on_success")
        Ensure.is_function(on_failed, "on_failed")

        url = "{0}/projections/continuous".format(self.base_url)
        self.tornado_http_sender.send_async(url, "GET", self.headers, None,
            lambda s: self.on_get_continuous(s, on_success, on_failed))

    def on_get_continuous(self, response, on_success, on_failed):
        if response.status == "OK":
            on_success(response)
            return
        on_failed(response)


########################################################################################################################


    def post_continuous(self, query, name=None, type=None, enabled=None, emit=None):
        queue = deque()
        on_success = lambda x: queue.append(self.sync_success(x))
        on_failed = lambda x: queue.append(self.sync_failed(x))
        self.start_post_contionuous_async(name, query, type, enabled, emit, on_success, on_failed)
        self.wait()
        result = queue.popleft()
        if result.success:
            return result.response
        else:
            raise result.response

    def start_post_contionuous_async(self, name, query, type, enabled, emit, on_success, on_failed):
        Ensure.is_function(on_success, "on_success")
        Ensure.is_function(on_failed, "on_failed")

        url = Url("{0}/projections/continuous".format(self.base_url)).param("name", name).param("type", type).param("enabled", enabled).param("emit", emit).get_url()
        self.tornado_http_sender.send_async(url, "POST", self.headers, query,
            lambda s: self.on_post_continuous(s, on_success, on_failed))

    def on_post_continuous(self, response, on_success, on_failed):
        if response.status == "Created":
            on_success(response)
            return
        on_failed(response)


########################################################################################################################


    def query_get(self, name = None, config = True):
        queue = deque()
        on_success = lambda x: queue.append(self.sync_success(x))
        on_failed = lambda x: queue.append(self.sync_failed(x))
        self.start_query_get_async(name, config, on_success, on_failed)
        self.wait()
        result = queue.popleft()
        if result.success:
            return result.response
        else:
            raise result.response

    def start_query_get_async(self, name, config, on_success, on_failed):
        Ensure.is_function(on_success, "on_success")
        Ensure.is_function(on_failed, "on_failed")
        Ensure.is_string(name, "name")

        url = Url("{0}/projection/{1}/query".format(self.base_url, name)).param("config", config).get_url()
        self.tornado_http_sender.send_async(url, "GET", self.headers, None,
            lambda s: self.on_query_get(s, on_success, on_failed))

    def on_query_get(self, response, on_success, on_failed):
        if response.status == "OK":
            on_success(json.loads(response))
            return
        on_failed(response)


########################################################################################################################


    def query_put(self, name, query, type = None, emit = None):
        queue = deque()
        on_success = lambda x: queue.append(self.sync_success(x))
        on_failed = lambda x: queue.append(self.sync_failed(x))
        self.start_query_put_async(name, query, type, emit, on_success, on_failed)
        self.wait()
        result = queue.popleft()
        if result.success:
            return result.response
        else:
            raise result.response

    def start_query_put_async(self,name, query, type, emit, on_success, on_failed):
        Ensure.is_not_empty_string(name, "name")
        Ensure.is_not_empty_string(query, "query")
        Ensure.is_function(on_success, "on_success")
        Ensure.is_function(on_failed, "on_failed")

        url = Url("{0}/projection/{1}/query".format(self.base_url, name)).param("type", type).param("emit", emit).get_url()
        self.tornado_http_sender.send_async(url, "POST", self.headers, query,
            lambda s: self.on_query_put(s, on_success, on_failed))

    def on_query_put(self, response, on_success, on_failed):
        if response.status == "OK":
            on_success(response)
            return
        on_failed(response)

########################################################################################################################


    def status_get(self, name):
        queue = deque()
        on_success = lambda x: queue.append(self.sync_success(x))
        on_failed = lambda x: queue.append(self.sync_failed(x))
        self.start_status_get_async(name, on_success, on_failed)
        self.wait()
        result = queue.popleft()
        if result.success:
            return result.response
        else:
            raise result.response

    def start_status_get_async(self, name, on_success, on_failed):
        Ensure.is_function(on_success, "on_success")
        Ensure.is_function(on_failed, "on_failed")
        Ensure.is_string(name, "name")

        url = "{0}/projection/{1}".format(self.base_url, name)
        self.tornado_http_sender.send_async(url, "GET", self.headers, None,
            lambda s: self.on_status_get(s, on_success, on_failed))

    def on_status_get(self, response, on_success, on_failed):
        if response.status == "OK":
            on_success(response)
            return
        on_failed(response)


########################################################################################################################


    def delete(self, name, deleteStateStream=None, deleteCheckpointStream=None):#?deleteStateStream={deleteStateStream}&deleteCheckpointStream={deleteCheckpointStream}
        queue = deque()
        on_success = lambda x: queue.append(self.sync_success(x))
        on_failed = lambda x: queue.append(self.sync_failed(x))
        self.start_delete_async(name,deleteStateStream, deleteCheckpointStream, on_success, on_failed)
        self.wait()
        result = queue.popleft()
        if result.success:
            return result.response
        else:
            raise result.response

    def start_delete_async(self, name, deleteStateStream, deleteCheckpointStream, on_success, on_failed):
        Ensure.is_not_empty_string(name, "name")
        Ensure.is_function(on_success, "on_success")
        Ensure.is_function(on_failed, "on_failed")

        url = Url("{0}/projection/{1}".format(self.base_url, name)).param("deleteStateStream", deleteStateStream).param("deleteCheckpointStream", deleteCheckpointStream).get_url()
        self.tornado_http_sender.send_async(url, "DELETE", self.headers, None,
            lambda s: self.on_delete(s, on_success, on_failed))

    def on_delete(self, response, on_success, on_failed):
        if response.status == "DELETED":
            on_success(response)
            return
        on_failed(response)

########################################################################################################################


    def statistics_get(self, name):
        queue = deque()
        on_success = lambda x: queue.append(self.sync_success(x))
        on_failed = lambda x: queue.append(self.sync_failed(x))
        self.start_statistics_get_async(name, on_success, on_failed)
        self.wait()
        result = queue.popleft()
        if result.success:
            return result.response
        else:
            raise result.response

    def start_statistics_get_async(self, name, on_success, on_failed):
        Ensure.is_function(on_success, "on_success")
        Ensure.is_function(on_failed, "on_failed")
        Ensure.is_string(name, "name")

        url = "{0}/projection/{1}/statistics".format(self.base_url, name)
        self.tornado_http_sender.send_async(url, "GET", self.headers, None,
            lambda s: self.on_statistics_get(s, on_success, on_failed))

    def on_statistics_get(self, response, on_success, on_failed):
        if response.status == "OK":
            on_success(response)
            return
        on_failed(response)


########################################################################################################################


    def state_get(self, name, partition=None):
        queue = deque()
        on_success = lambda x: queue.append(self.sync_success(x))
        on_failed = lambda x: queue.append(self.sync_failed(x))
        self.start_state_get_async(name, partition, on_success, on_failed)
        self.wait()
        result = queue.popleft()
        if result.success:
            return result.response
        else:
            raise result.response

    def start_state_get_async(self, name, partition, on_success, on_failed):
        Ensure.is_function(on_success, "on_success")
        Ensure.is_function(on_failed, "on_failed")
        Ensure.is_string(name, "name")

        url = Url("{0}/projection/{1}/state".format(self.base_url, name)).param("partition", partition).get_url()
        self.tornado_http_sender.send_async(url, "GET", self.headers, None,
            lambda s: self.on_state_get(s, on_success, on_failed))

    def on_state_get(self, response, on_success, on_failed):
        if response.status == "OK":
            on_success(response)
            return
        on_failed(response)


########################################################################################################################


class Url:
    def __init__(self, baseUrl):
        self.url = baseUrl
        self.has_params = False

    def param(self, name, value):
        if value is  not None:
            if not self.has_params:
                self.url+="?{0}={1}".format(name, str(value))
                self.has_params = True
            else:
                self.url+="&{0}={1}".format(name, str(value))
        return self

    def get_url(self):
        return self.url


########################################################################################################################