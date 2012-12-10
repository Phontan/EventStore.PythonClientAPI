import sys, os
sys.path.append(os.path.dirname(__file__)+"\\libs");
import tornado.httpclient

class TornadoHttpSender:
    def SendAsync(self,url, method, headers, body, call_back):
        http_client = tornado.httpclient.AsyncHTTPClient()
        request = tornado.httpclient.HTTPRequest(url, method=method, headers=headers, body=body)
        http_client.fetch(request, call_back)
