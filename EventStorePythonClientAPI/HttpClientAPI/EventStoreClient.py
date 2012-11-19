import sys;
import http.client;
from Event import *
from ClientJsonSerelizationOption import *
sys.path.append("D:\\apps\\EventStore.PythonClientAPI\\EventStorePythonClientAPI\\HttpClientAPI\\Body")
from CreateStreamRequestBody import *
from AppendToStreamRequestBody import *
from DeleteStreamRequestBody import *
import socket
import concurrent
import urllib.request
import urllib.parse
from HttpSendRequest import HttpSendRequest

class EventStoreClient:
    def __init__(self, ipAddress="http://127.0.0.1", port = 2113, contentType = "application/json", accept = "application/json", extensions = "json"):
        self.__ipAddress = ipAddress;
        self.__port = port;
        self.__baseUrl = ipAddress+':'+str(port);
        self.__headers = {"content-type" : contentType,"accept" :  accept,"extensions" : extensions};
        self.httpSendRequest = HttpSendRequest()

    def CreateStream(self, streamId, metadeta):
        body = CreateStreamRequestBody(streamId, metadeta)
        data = json.dumps(body, default=convert_to_builtin_type)
        url = self.__baseUrl+"/streams";
        return self.httpSendRequest.Send("POST", url,self.__headers,data);

##    def GetStreams(self):
##        url = self.__baseUrl+"/streams"
##        request = urllib.request.Request(url, headers = self.__headers)
##        response = urllib.request.urlopen(request)
##        return responce;

    def DeleteStream(self,streamId , expectedVersion=-2):
        url = self.__baseUrl+"/streams/"+streamId;
        body = json.dumps(DeleteStreamRequestBody(expectedVersion), default=convert_to_builtin_type);
        return self.httpSendRequest.Send("DELETE", url, self.__headers, body);

    def ReadEvent(self,streamId , eventId):
        url = self.__baseUrl+"/streams/"+streamId+"/event/"+str(eventId)+"?resolve="+"yes"
        responce = self.httpSendRequest.Send("GET", url, self.__headers)
        responceContent = responce.read().decode('utf-8');
        if responceContent == '':
            return;
        event = json.loads(responceContent)
        return event;

    def AppendToStream(self,streamId, data, expectedVersion=-2):
        if(type(list())!= type(data)):
            newData = list();
            newData.append(data);
            data = newData;
        data = AppendToStreamRequestBody(expectedVersion, data)
        data  = json.dumps(data, default=convert_to_builtin_type)
        url = self.__baseUrl+"/streams/"+streamId
        return self.httpSendRequest.Send("POST", url, self.__headers, data)

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
##
##    def ReadStreamEventsForward(self, streamId, startPosition, count):
##        result = self.ReadStreamEventsBackward(streamId, count+startPosition-1, count);
##        result = list(reversed(result));
##        return result;
##
##    def ReadAllEventsBackward(self,preparePosition, commitPosition, count):
##        self.__Connect();
##        hexStartPosition = self.__ConvrtToHex16(preparePosition) + self.__ConvrtToHex16(commitPosition);
##        self.__connection.request("GET", "/streams/$all/before/"+hexStartPosition+"/"+str(count),headers=self.__headers);
##        readLine = self.__connection.getresponse().read().decode('utf-8');
##        if readLine == "":
##            return;
##        responce = json.loads(readLine);
##
##        newPreparePosition = self.__GetPreparePosition(responce['links'][3]["uri"]);
##        newCommitPosition = self.__GetCommitPosition(responce['links'][3]["uri"])
##        events = [];
##        for uri in responce['entries']:
##            self.__connection.request("GET", uri['links'][2]['uri'], headers = self.__headers);
##            readObj = self.__connection.getresponse().read().decode('utf-8');
##            if readObj!='':
##                events.append( json.loads(readObj));
##        self.__Disconnect();
##        return {"preparePosition": newPreparePosition, "commitPosition" : newCommitPosition, "events":events};
##
##    def ReadAllEventsForward(self,preparePosition, commitPosition, count):
##        self.__Connect();
##        hexStartPosition = self.__ConvrtToHex16(preparePosition) + self.__ConvrtToHex16(commitPosition);
##        self.__connection.request("GET", "/streams/$all/after/"+hexStartPosition+"/"+str(count),headers=self.__headers);
##        readLine = self.__connection.getresponse().read().decode('utf-8');
##        if readLine == "":
##            return;
##        responce = json.loads(readLine);
##
##        newPreparePosition = self.__GetPreparePosition(responce['links'][3]["uri"]);
##        newCommitPosition = self.__GetCommitPosition(responce['links'][3]["uri"])
##        events = [];
##        for uri in responce['entries']:
##            self.__connection.request("GET", uri['links'][2]['uri'], headers = self.__headers);
##            readObj = self.__connection.getresponse().read().decode('utf-8');
##            if readObj!='':
##                events.append( json.loads(readObj));
##        self.__Disconnect();
##        return {"preparePosition": newPreparePosition, "commitPosition" : newCommitPosition, "events":list(reversed(events))};
##
##    def ReadAllEvents(self):
##        self.__Connect();
##        self.__connection.request("GET","/streams/$all",headers = self.__headers );
##        readLine = self.__connection.getresponse().read().decode('utf-8');
##        if readLine == "":
##            return;
##        responce = json.loads(readLine);
##        newPreparePosition = self.__GetPreparePosition(responce['links'][3]["uri"]);
##        newCommitPosition = self.__GetCommitPosition(responce['links'][3]["uri"]);
##        events = [];
##        for uri in responce['entries']:
##            self.__connection.request("GET", uri['links'][2]['uri'], headers = self.__headers);
##            readObj = self.__connection.getresponse().read().decode('utf-8');
##            if readObj!='':
##                events.append( json.loads(readObj));
##        self.__Disconnect();
##        return {"preparePosition": newPreparePosition, "commitPosition" : newCommitPosition, "events": events};

    def __GetPreparePosition(self, link):
        position = link.split('/')[6];
        result =int(position[0:16], 16);
        return result;

    def __GetCommitPosition(self, link):
        position = link.split('/')[6];
        result =int(position[16:32], 16);
        return result;

    def __GetStreamsCount(self, streamId):
        self.__Connect();
        self.__connection.request("GET", "/streams/"+streamId,headers=self.__headers);
        summary = json.loads(self.__connection.getresponse().read().decode('utf-8'))["entries"][0]["summary"];
        self.__Disconnect();
        return int(str.split(summary, " #")[1]);

    def __ConvrtToHex16(self, number):
        if number<0:
            hexVal = hex(number & 0xffffffffffffffff);
        else: hexVal = hex(number);
        number = str(hexVal).split('x')[1];
        while len(number)!=16:
            number = "0"+number;
        return number;