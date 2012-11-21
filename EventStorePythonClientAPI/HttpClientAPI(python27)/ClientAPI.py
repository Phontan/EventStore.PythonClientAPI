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
        SendAsync(url, "POST", self.__headers, body,lambda x: self.__CreateStreamAsyncCallback(x, onSuccess, onFailed));
    def __CreateStreamAsyncCallback(self, response, onSuccess,onFailed):
        if response.code==201:
            onSuccess(SimplyAnswer(201, response.body));
        else:
            onFailed(FailedAnswer(response.code,response.error.message));

    def DeleteStreamAsync(self,streamId , onSuccess, onFailed,expectedVersion=-2):
        url = self.__baseUrl+"/streams/"+streamId;
        body = json.dumps(DeleteStreamRequestBody(expectedVersion), default=convert_to_builtin_type);
        SendAsync(url, "DELETE", self.__headers, body, lambda x: self.__DeleteStreamCallback(x, onSuccess, onFailed));
    def __DeleteStreamCallback(self, response, onSuccess,onFailed):
        if response.code==204:
            onSuccess(SimplyAnswer(response.code, response.body));
        else:
            a = FailedAnswer(response.code,response.error.message)
            onFailed(FailedAnswer(response.code,response.error.message));

    def AppendToStreamAsync(self,streamId, data, onSuccess, onFailed, expectedVersion=-2):
        if(type(list())!= type(data)):
            newData = list();
            newData.append(data);
            data = newData;
        body = json.dumps(AppendToStreamRequestBody(expectedVersion, data), default=convert_to_builtin_type)
        url = self.__baseUrl+"/streams/"+streamId
        SendAsync(url,"POST", self.__headers, body, lambda x: self.__AppendToStreamCallback(x, onSuccess, onFailed))
    def __AppendToStreamCallback(self, response, onSuccess,onFailed):
        if response.code==201:
            onSuccess(SimplyAnswer(response.code, response.body));
        else:
            onFailed(FailedAnswer(response.code,response.error.message))

    def ReadEventAsync(self,streamId , eventId,  onSuccess, onFailed):
        url = self.__baseUrl+"/streams/"+streamId+"/event/"+str(eventId)+"?resolve="+"yes"
        SendAsync(url, "GET", self.__headers, None, lambda x: self.__ReadEventCallback(x, onSuccess, onFailed))
    def __ReadEventCallback(self, response, onSuccess, onFailed):
        if response.code!=200:
            onFailed(FailedAnswer(response.code,response.error.message))
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
        SendAsync(url, "GET", self.__headers, None,lambda x: self.__ReadStreamEventsBackwardCallback(x, onSuccess, onFailed))
    def __ReadStreamEventsBackwardCallback(self, response, onSuccess,onFailed):
        if response.code!=200:
            onFailed(FailedAnswer(response.code,response.error.message))
            return;
        response = json.loads(response.body);
        events = [];
        eventsCount=0
        try:
            for uri in response['entries']:
                url = uri['links'][2]['uri'];
                SendAsync(url, "GET", self.__headers, None,lambda x: self.__EventReadCallback(x, events,eventsCount, onSuccess, onFailed))
                eventsCount+=1
        except:
            onFailed(FailedAnswer(response.code,response.error.message))
            return;
    def __EventReadCallback(self, response, events,eventsCount, onSuccess, onFailed):
        try:
            events.append(json.loads(response.body))
            if len(events)==eventsCount:
                onSuccess(EventsAnswer(events))
        except:
            onFailed(FailedAnswer(response.code,response.error.message))

    def ReadStreamEventsForwardAsync(self, streamId, startPosition, count,  onSuccess, onFailed):
        url = self.__baseUrl+"/streams/"+streamId+"/range/"+str(startPosition+count)+"/"+str(count);
        SendAsync(url, "GET", self.__headers, None,lambda x: self.__ReadStreamEventsBackwardCallback(x, onSuccess, onFailed))

    def __GetPreparePosition(self, link):
        position = link.split('/')[6];
        result =int(position[0:16], 16);
        return result;

    def __GetCommitPosition(self, link):
        position = link.split('/')[6];
        result =int(position[16:32], 16);
        return result;

    def __ConvrtToHex16(self, number):
        if number<0:
            hexVal = hex(number & 0xffffffffffffffff).split('L')[0];
        else: hexVal = hex(number);
        number = str(hexVal).split('x')[1];
        while len(number)!=16:
            number = "0"+number;
        return number;

    def ReadAllEventsBackwardAsync(self,preparePosition, commitPosition, count, onSuccess, onFailed):
        hexStartPosition = self.__ConvrtToHex16(preparePosition) + self.__ConvrtToHex16(commitPosition);
        url = self.__baseUrl+"/streams/$all/before/"+hexStartPosition+"/"+str(count);
        SendAsync(url, "GET", self.__headers, None, lambda response: self.__ReadAllEventsAsyncCallback(response, onSuccess, onFailed))

    def ReadAllEventsForwardAsync(self,preparePosition, commitPosition, count, onSuccess, onFailed):
        hexStartPosition = self.__ConvrtToHex16(preparePosition) + self.__ConvrtToHex16(commitPosition);
        url = self.__baseUrl+"/streams/$all/after/"+hexStartPosition+"/"+str(count);
        SendAsync(url, "GET", self.__headers, None, lambda response: self.__ReadAllEventsAsyncCallback(response, onSuccess, onFailed))

    def __ReadAllEventsAsyncCallback(self, response, onSuccess, onFailed):
        if response.code!=200:
            onFailed(FailedAnswer(response.code,response.error.message))
        readLine = response.body;
        if readLine == "":
            onSuccess(AllEventsAnswer("", -1, -1))
            return;
        body = json.loads(readLine);
        newPreparePosition = self.__GetPreparePosition(body['links'][3]["uri"]);
        newCommitPosition = self.__GetCommitPosition(body['links'][3]["uri"])
        events = [];
        eventsCount=0
        try:
            if body['entries'] == []:
                onSuccess(AllEventsAnswer("", -1, -1))
                return;
            for uri in body['entries']:
                url = uri['links'][2]['uri'];
                SendAsync(url, "GET", self.__headers, None,lambda x: self.__ReadAllEvents(x, events,eventsCount, onSuccess, onFailed, newPreparePosition, newCommitPosition))
                eventsCount+=1
        except:
            onFailed(FailedAnswer(response.code,response.error.message))
            return;
    def __ReadAllEvents(self, response, events,eventsCount, onSuccess, onFailed, newPreparePosition, newCommitPosition):
        if response.code !=200:
            onFailed(FailedAnswer(response.code,response.error.message))
        try:
            events.append(json.loads(response.body))
            if len(events)==eventsCount:
                onSuccess(AllEventsAnswer(events, newPreparePosition, newCommitPosition))
        except:
            onFailed(FailedAnswer(response.code,response.error.message))