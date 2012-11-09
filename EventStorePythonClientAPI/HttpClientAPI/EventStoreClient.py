import sys;
import http.client;
from Event import *;
import time;


class EventStoreClient:
    __headers ={"content-type" : "application/json","accept" :  "application/json","extensions" : "json"};

    def __init__(self, ipAddress="127.0.0.1", port = 2113 ):
        self.__ipAddress = ipAddress;
        self.__port = port;

    def __Connect(self):
        self.__connection = http.client.HTTPConnection(self.__ipAddress,self.__port);
        self.__connection.connect();

    def __Disconnect(self):
        self.__connection.close();

    def CreateStream(self, streamId, metadeta):
        body = "{\"eventStreamId\": \""+streamId+"\",  \"metadata\": \""+metadeta+"\"}"
        self.__Connect();
        self.__connection.request("POST", "/streams", body, self.__headers);
        responce = self.__connection.getresponse();
        self.__Disconnect();
        return responce;

    def GetStreams(self):
        self.__connection.request("GET", "/streams",headers =  self.__headers);
        self.__Connect();
        responce = self.__connection.getresponse();
        self.__Disconnect();
        return responce;

    def DeleteStream(self,streamId , expectedVersion=-2):
        body = "{\"expectedVersion\": \""+str(expectedVersion)+"\"}";
        self.__Connect();
        self.__connection.request("DELETE", "/streams/"+streamId, body, self.__headers);
        responce = self.__connection.getresponse();
        self.__Disconnect();
        return responce;

    def ReadEvent(self,streamId , eventId):
        self.__Connect();
        self.__connection.request("GET", "/streams/"+streamId+"/event/"+str(eventId)+"?resolve="+"yes",headers=self.__headers);
        responce = self.__connection.getresponse();
        responceContent = responce.read().decode('utf-8');
        if responceContent == '':
            return;
        event = json.loads(responceContent)
        self.__Disconnect();
        return event;

    def AppendToStream(self,streamId, data, expectedVersion):
        if(type(list())!= type(data)):
            newData = list();
            newData.append(data);
            data = newData;
        body = "{\"expectedVersion\":"+str(expectedVersion)+",\"events\":"+json.dumps(data, default =convert_to_builtin_type)+"}";
        self.__Connect();
        self.__connection.request("POST", "/streams/"+streamId,body = body,headers=self.__headers);
        responce = self.__connection.getresponse();
        self.__Disconnect();
        return responce;

    def ReadStreamEventsBackward(self, streamId, startPosition, count):
        self.__Connect();
        self.__connection.request("GET", "/streams/"+streamId+"/range/"+str(startPosition)+"/"+str(count),headers=self.__headers);
        responce = json.loads(self.__connection.getresponse().read().decode('utf-8'));
        events = [];
        for uri in responce['entries']:
            self.__connection.request("GET", uri['links'][2]['uri'], headers = self.__headers);
            readObj = self.__connection.getresponse().read().decode('utf-8');
            events.append( json.loads(readObj));
        self.__Disconnect();
        return events;

    def ReadStreamEventsForward(self, streamId, startPosition, count):
        result = self.ReadStreamEventsBackward(streamId, count+startPosition-1, count);
        result = list(reversed(result));
        return result;

    def ReadAllEventsBackward(self,preparePosition, commitPosition, count):
        self.__Connect();
        hexStartPosition = self.__ConvrtToHex16(preparePosition) + self.__ConvrtToHex16(commitPosition);
        self.__connection.request("GET", "/streams/$all/before/"+hexStartPosition+"/"+str(count),headers=self.__headers);
        readLine = self.__connection.getresponse().read().decode('utf-8');
        if readLine == "":
            return;
        responce = json.loads(readLine);

        newPreparePosition = self.__GetPreparePosition(responce['links'][3]["uri"]);
        newCommitPosition = self.__GetCommitPosition(responce['links'][3]["uri"])
        events = [];
        for uri in responce['entries']:
            self.__connection.request("GET", uri['links'][2]['uri'], headers = self.__headers);
            readObj = self.__connection.getresponse().read().decode('utf-8');
            if readObj!='':
                events.append( json.loads(readObj));
        self.__Disconnect();
        return {"preparePosition": newPreparePosition, "commitPosition" : newCommitPosition, "events":events};

    def ReadAllEventsForward(self,preparePosition, commitPosition, count):
        self.__Connect();
        hexStartPosition = self.__ConvrtToHex16(preparePosition) + self.__ConvrtToHex16(commitPosition);
        self.__connection.request("GET", "/streams/$all/after/"+hexStartPosition+"/"+str(count),headers=self.__headers);
        readLine = self.__connection.getresponse().read().decode('utf-8');
        if readLine == "":
            return;
        responce = json.loads(readLine);

        newPreparePosition = self.__GetPreparePosition(responce['links'][3]["uri"]);
        newCommitPosition = self.__GetCommitPosition(responce['links'][3]["uri"])
        events = [];
        for uri in responce['entries']:
            self.__connection.request("GET", uri['links'][2]['uri'], headers = self.__headers);
            readObj = self.__connection.getresponse().read().decode('utf-8');
            if readObj!='':
                events.append( json.loads(readObj));
        self.__Disconnect();
        return {"preparePosition": newPreparePosition, "commitPosition" : newCommitPosition, "events":list(reversed(events))};

    def ReadAllEvents(self):
        self.__Connect();
        self.__connection.request("GET","/streams/$all",headers = self.__headers );
        readLine = self.__connection.getresponse().read().decode('utf-8');
        if readLine == "":
            return;
        responce = json.loads(readLine);
        newPreparePosition = self.__GetPreparePosition(responce['links'][3]["uri"]);
        newCommitPosition = self.__GetCommitPosition(responce['links'][3]["uri"]);
        events = [];
        for uri in responce['entries']:
            self.__connection.request("GET", uri['links'][2]['uri'], headers = self.__headers);
            readObj = self.__connection.getresponse().read().decode('utf-8');
            if readObj!='':
                events.append( json.loads(readObj));
        self.__Disconnect();
        return {"preparePosition": newPreparePosition, "commitPosition" : newCommitPosition, "events": events};

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