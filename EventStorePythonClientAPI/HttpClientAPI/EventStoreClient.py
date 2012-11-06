import sys;
import http.client;
import Event;


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
        return self.__connection.getresponse();

    def DeleteStream(self,streamId , expectedVersion):
        body = "{\"expectedVersion\": \""+str(expectedVersion)+"\"}"
        self.__connection.request("DELETE", "/streams/"+streamId, body, self.__headers);
        return self.__connection.getresponse();

    def ReadEvent(self,streamId , eventId):
        self.__connection.request("GET", "/streams/"+streamId+"/event/"+str(eventId)+"?resolve="+"yes",headers=self.__headers);
        return self.__connection.getresponse();

    def AppendToStream(self,streamId, data, expectedVersion):
        body = "{\"expectedVersion\":"+str(expectedVersion)+",\"events\":"+Event.json.dumps(data, default =Event.convert_to_builtin_type)+"}";
        self.__connection.request("POST", "/streams/"+streamId,body = body,headers=self.__headers);
        return self.__connection.getresponse();