from Event import *
from ClientJsonSerelizationOption import *
import sys
sys.path.append("D:\\apps\\EventStore.PythonClientAPI\\EventStorePythonClientAPI\\HttpClientAPI(python27)\\libs");
from bodyLibs import *
from answerLibs import *
from AsyncRequestSender import *

class ClientAPI:
    def __init__(self, ipAddress="http://127.0.0.1", port = 2113, contentType = "application/json", accept = "application/json", extensions = "json"):
        self.__baseUrl = ipAddress+':'+str(port);
        self.__headers = {"content-type" : contentType,"accept" :  accept,"extensions" : extensions};

    def CreateStreamAsync(self, streamId, metadata, onSuccess, onFailed):
        body = json.dumps(CreateStreamRequestBody(streamId, metadata), default=convert_to_builtin_type)
        url = self.__baseUrl+"/streams";
        SendAsync(url, "POST", self.__headers, body,lambda x: self.CreateStreamCallback(x, onSuccess, onFailed));
    def CreateStreamCallback(self, response, onSuccess,onFailed):
        if response.code==201:
            onSuccess(SimplyAnswer(201, response.body));
        else:
            onFailed(ErrorAnswer(response.error));

    def DeleteStreamAsync(self,streamId , onSuccess, onFailed,expectedVersion=-2):
        url = self.__baseUrl+"/streams/"+streamId;
        body = json.dumps(DeleteStreamRequestBody(expectedVersion), default=convert_to_builtin_type);
        SendAsync(url, "DELETE", self.__headers, body, lambda x: self.DeleteStreamCallback(x, onSuccess, onFailed));
    def DeleteStreamCallback(self, response, onSuccess,onFailed):
        if response.code==204:
            onSuccess(SimplyAnswer(response.code, response.body));
        else:
            a = ErrorAnswer(response.error.__doc__)
            onFailed(ErrorAnswer(response.error));

    def AppendToStreamAsync(self,streamId, data, onSuccess, onFailed, expectedVersion=-2):
        if(type(list())!= type(data)):
            newData = list();
            newData.append(data);
            data = newData;
        body = json.dumps(AppendToStreamRequestBody(expectedVersion, data), default=convert_to_builtin_type)
        url = self.__baseUrl+"/streams/"+streamId
        SendAsync(url,"POST", self.__headers, body, lambda x: self.AppendToStreamCallback(x, onSuccess, onFailed))
    def AppendToStreamCallback(self, response, onSuccess,onFailed):
        if response.code==201:
            onSuccess(SimplyAnswer(response.code, response.body));
        else:
            onFailed(ErrorAnswer(response.error));

    def ReadEventAsync(self,streamId , eventId,  onSuccess, onFailed):
        url = self.__baseUrl+"/streams/"+streamId+"/event/"+str(eventId)+"?resolve="+"yes"
        SendAsync(url, "GET", self.__headers, None, lambda x: self.ReadEventCallback(x, onSuccess, onFailed))
    def ReadEventCallback(self, response, onSuccess, onFailed):
        if response.code!=200:
            onFailed(ErrorAnswer(response.error));
            return;
        responseContent = response.body
        if responseContent == '':
            onSuccess(EventsAnswer(""))
            return;
        event = json.loads(responseContent)
        a=EventsAnswer(event)
        onSuccess(EventsAnswer(event))

    def ReadStreamEventsBackwardAsync(self, streamId, startPosition, count,  onSuccess, onFailed):
        url = self.__baseUrl+"/streams/"+streamId+"/range/"+str(startPosition)+"/"+str(count);
        SendAsync(url, "GET", self.__headers, None,lambda x: call_back(self.ReadStreamEventsBackwardCallback(x)))
    def ReadStreamEventsBackwardCallback(self, response, onSuccess,onFailed):
        if response.code!=200:
            return;
        response = json.loads(response.body);
        events = [];
        for uri in response['entries']:
            url = uri['links'][2]['uri'];
            SendAsync(url, "GET", None, self.__headers, )
        return events;


##    def ReadStreamEventsBackward(self, streamId, startPosition, count):
##        self.__Connect();
##        self.__connection.request("GET", "/streams/"+streamId+"/range/"+str(startPosition)+"/"+str(count),headers=self.__headers);
##        responce = json.loads(self.__connection.getresponse().read().decode('utf-8'));
##        events = [];
##        for uri in responce['entries']:
##            self.__connection.request("GET", uri['links'][2]['uri'], headers = self.__headers);
##            readObj = self.__connection.getresponse().read().decode('utf-8');
##            events.append( json.loads(readObj));
##        self.__Disconnect();
##        return events;