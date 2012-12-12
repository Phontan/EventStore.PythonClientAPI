import sys, os
sys.path.append(os.path.dirname(__file__)+"\\libs");
import tornado.httpclient

class TornadoHttpSender:
    def SendAsync(self,url, method, headers, body, call_back,connect_timeout=20000, request_timeout=2000):
        http_client = tornado.httpclient.AsyncHTTPClient()
        if method == 'DELETE':
            request = tornado.httpclient.HTTPRequest(url, method=method, headers=headers, body = body, allow_nonstandard_methods = True)
        else:
            request = tornado.httpclient.HTTPRequest(url, method=method, headers=headers, body=body,connect_timeout=connect_timeout, request_timeout=request_timeout)
        http_client.fetch(request, call_back)
