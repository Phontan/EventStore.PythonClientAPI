import urllib.request
import socket
import sys
sys.path.append("D:\\apps\\EventStore.PythonClientAPI\\EventStorePythonClientAPI\\HttpClientAPI\\Exceptions")
from ConnectToServerException import *

class HttpSendRequest:
    def Send(self,requestType, requestUrl, requestHeader,requestBody=""):
        if requestBody=="":
            request = urllib.request.Request(requestUrl, headers = requestHeader)
        else:
            request = urllib.request.Request(requestUrl, requestBody.encode('utf-8'), requestHeader)
            request.get_method = lambda: requestType
        try:
            response = urllib.request.urlopen(request)
        except socket.error as err:
            raise ConnectToServerException(err, "Error during connectiong to server. ")
        return response;
