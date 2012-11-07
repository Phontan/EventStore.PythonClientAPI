import sys;
import http.client;
from Event import *;


class EventStoreClient:
    __headers ={"content-type" : "application/json","accept" :  "application/json","extensions" : "json"};

    def __init__(self, ipAddress="127.0.0.1", port = 2113 ):
        self.__connection = http.client.HTTPConnection(ipAddress,port);
        self.__connection.connect();

    def CreateStream(self, streamId, metadeta):
        body = "{\"eventStreamId\": \""+streamId+"\",  \"metadata\": \""+metadeta+"\"}"
        self.__connection.request("POST", "/streams", body, self.__headers);
        return self.__connection.getresponse();

    def GetStreams(self):
        self.__connection.request("GET", "/streams",headers =  self.__headers);
        responce = self.__connection.getresponse();
        return responce;

    def DeleteStream(self,streamId , expectedVersion=-2):
        body = "{\"expectedVersion\": \""+str(expectedVersion)+"\"}"
        self.__connection.request("DELETE", "/streams/"+streamId, body, self.__headers);
        return self.__connection.getresponse();

    def ReadEvent(self,streamId , eventId):
        self.__connection.request("GET", "/streams/"+streamId+"/event/"+str(eventId)+"?resolve="+"yes",headers=self.__headers);
        return self.__connection.getresponse();

    def AppendToStream(self,streamId, data, expectedVersion):
        body = "{\"expectedVersion\":"+str(expectedVersion)+",\"events\":"+json.dumps(data, default =convert_to_builtin_type)+"}";
        self.__connection.request("POST", "/streams/"+streamId,body = body,headers=self.__headers);
        return self.__connection.getresponse();

    def ReadStreamEventsBackward(self, streamId, startPosition, count):
        self.__connection.request("GET", "/streams/"+streamId+"/range/"+str(startPosition + count-1)+"/"+str(count),headers=self.__headers);
        responce = json.loads(self.__connection.getresponse().read().decode('utf-8'));
        events = [];
        for uri in responce['entries']:
            self.__connection.request("GET", uri['links'][2]['uri'], headers = self.__headers);
            readObj = self.__connection.getresponse().read().decode('utf-8');
            events.append( json.loads(readObj));
        return events;

    def ReadStreamEventsForward(self, streamId, startPosition, count):
        self.__connection.request("GET", "/streams/"+streamId+"/range/"+str(count+startPosition)+"/"+str(count),headers=self.__headers);
        responce = json.loads(self.__connection.getresponse().read().decode('utf-8'));
        events = [];
        for uri in responce['entries']:
            self.__connection.request("GET", uri['links'][2]['uri'], headers = self.__headers);
            readObj = self.__connection.getresponse().read().decode('utf-8');
            events.append( json.loads(readObj));
        return list.reverse(events);

    def ReadAllEventsBackward(self,preparePosition, commitPosition, count):
        hexStartPosition = self.__ConvrtToHex16(preparePosition) + self.__ConvrtToHex16(commitPosition);
        self.__connection.request("GET", "/streams/$all/before/"+hexStartPosition+"/"+str(count),headers=self.__headers);
        readLine = self.__connection.getresponse().read().decode('utf-8');
        if readLine == "":
            return;
        responce = json.loads(readLine);
        events = [];
        for uri in responce['entries']:
            self.__connection.request("GET", uri['links'][2]['uri'], headers = self.__headers);
            readObj = self.__connection.getresponse().read().decode('utf-8');
            if readObj!='':
                events.append( json.loads(readObj));
        return events;

    def ReadAllEventsForward(self,preparePosition, commitPosition, count):
        hexStartPosition = self.__ConvrtToHex16(preparePosition) + self.__ConvrtToHex16(commitPosition);
        self.__connection.request("GET", "/streams/$all/after/"+hexStartPosition+"/"+str(count),headers=self.__headers);
        readLine = self.__connection.getresponse().read().decode('utf-8');
        if readLine == "":
            return;
        responce = json.loads(readLine);
        events = [];
        for uri in responce['entries']:
            self.__connection.request("GET", uri['links'][2]['uri'], headers = self.__headers);
            readObj = self.__connection.getresponse().read().decode('utf-8');
            if readObj!='':
                events.append( json.loads(readObj));
        return events;

    def __GetStreamsCount(self, streamId):
        self.__connection.request("GET", "/streams/"+streamId,headers=self.__headers);
        summary = json.loads(self.__connection.getresponse().read().decode('utf-8'))["entries"][0]["summary"];
        return int(str.split(summary, " #")[1]);

    def __ConvrtToHex16(self, number):
        number = str(hex(number)).split('x')[1];
        while len(number)!=16:
            number = "0"+number;
        return number;