from urllib import quote_plus
from  tornado.httpclient import *

class TornadoHttpSender:
    def send_async(self, url, method, headers, body, call_back,connect_timeout=20000, request_timeout=2000):
        http_client =  AsyncHTTPClient()
        #url = quote_plus(url)
        if method == 'DELETE':
            request = HTTPRequest(url, method=method, headers=headers, body = body, allow_nonstandard_methods = True)
        else:
            request = HTTPRequest(url, method=method, headers=headers, body=body,connect_timeout=connect_timeout, request_timeout=request_timeout)
        http_client.fetch(request, call_back)
