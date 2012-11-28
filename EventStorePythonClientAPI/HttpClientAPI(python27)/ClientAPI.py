from Event import *
from ClientJsonSerelizationOption import *
import sys
sys.path.append("D:\\apps\\EventStore.PythonClientAPI\\EventStorePythonClientAPI\\HttpClientAPI(python27)\\libs");
from bodyLibs import *
from answerLibs import *
from AsyncRequestSender import *
import Ensure
from threading import*
from ReadEventsData import *

class ClientAPI:
    def __init__(self, ipAddress="http://127.0.0.1", port = 2113):
        self.__baseUrl = ipAddress+':'+str(port);
        self.__headers = {"content-type" : "application/json","accept" :  "application/json","extensions" : "json"};
        self.__readBatchSize = 20

    def CreateStreamAsync(self, streamId, metadata, onSuccess, onFailed):
        Ensure.IsStringNotEmpty(streamId, "streamId")
        Ensure.IsString(metadata, "metadata")
        Ensure.IsFunction(onSuccess, "onSuccess")
        Ensure.IsFunction(onFailed, "onFailed")

        try:
            body = ToJson(CreateStreamRequestBody(streamId, metadata))
        except:
            raise;
        url = self.__baseUrl+"/streams";
        SendAsync(url, "POST", self.__headers, body,lambda x: self.__CreateStreamAsyncCallback(x, onSuccess, onFailed));
    def __CreateStreamAsyncCallback(self, response, onSuccess,onFailed):
        if response.code==201:
            onSuccess(SimplyAnswer(201, response.body));
        else:
            onFailed(FailedAnswer(response.code,response.error.message));

    def DeleteStreamAsync(self,streamId , onSuccess, onFailed,expectedVersion=-2):
        Ensure.IsStringNotEmpty(streamId, "streamId")
        Ensure.IsFunction(onSuccess, "onSuccess")
        Ensure.IsFunction(onFailed, "onFailed")
        Ensure.IsNumber(expectedVersion, "expectedVersion")

        url = self.__baseUrl+"/streams/"+streamId;
        try:
            body = ToJson(DeleteStreamRequestBody(expectedVersion));
        except:
            raise;
        SendAsync(url, "DELETE", self.__headers, body, lambda x: self.__DeleteStreamCallback(x, onSuccess, onFailed));
    def __DeleteStreamCallback(self, response, onSuccess,onFailed):
        if response.code==204:
            onSuccess(SimplyAnswer(response.code, response.body));
        else:
            onFailed(FailedAnswer(response.code,response.error.message));

    def AppendToStreamAsync(self,streamId, events, onSuccess, onFailed, expectedVersion=-2):
        Ensure.IsStringNotEmpty(streamId, "streamId")
        Ensure.IsFunction(onSuccess, "onSuccess")
        Ensure.IsFunction(onFailed, "onFailed")
        Ensure.IsNumber(expectedVersion, "expectedVersion")

        if(type(list())!= type(events)):
            newData = list();
            newData.append(events);
            events = newData;
        #check all events if they really are events
        try:
            body = ToJson(AppendToStreamRequestBody(expectedVersion, events))
        except:
            raise;
        url = self.__baseUrl+"/streams/"+streamId
        SendAsync(url,"POST", self.__headers, body, lambda x: self.__AppendToStreamCallback(x, onSuccess, onFailed))
    def __AppendToStreamCallback(self, response, onSuccess,onFailed):
        if response.code==201:
            onSuccess(SimplyAnswer(response.code, response.body));
        else:
            onFailed(FailedAnswer(response.code,response.error.message))

    def ReadEventAsync(self,streamId , eventNumber,  onSuccess, onFailed, resolve=1):
        Ensure.IsStringNotEmpty(streamId, "streamId")
        Ensure.IsFunction(onSuccess, "onSuccess")
        Ensure.IsFunction(onFailed, "onFailed")
        Ensure.IsNotNegativeNumber(eventNumber, "eventNumber")
        if resolve: resolve = "yes"
        else: resolve="no"
        url = self.__baseUrl+"/streams/"+streamId+"/event/"+str(eventNumber)+"?resolve="+resolve
        SendAsync(url, "GET", self.__headers, None, lambda x: self.__ReadEventCallback(x, onSuccess, onFailed))
    def __ReadEventCallback(self, response, onSuccess, onFailed):
        if response.code!=200:
            onFailed(FailedAnswer(response.code,response.error.message))
            return;
        responseContent = response.body
        try:
            event = json.loads(responseContent)
            onSuccess(EventsAnswer(event))
        except:
            raise;

################################################ Read Stream Evens Backward ###############
    def ReadStreamEventsBackwardAsync(self, streamId, startPosition, count,  onSuccess, onFailed):
        Ensure.IsStringNotEmpty(streamId, "streamId")
        Ensure.IsFunction(onSuccess, "onSuccess")
        Ensure.IsFunction(onFailed, "onFailed")
        Ensure.IsNotNegativeNumber(startPosition, "startPosition")
        Ensure.IsPositiveNumber(count, "count")

        events = []
        batchCounter=0
        readEventsData = ReadEventsData;
        readEventsData.streamId = streamId;
        readEventsData.startPosition = startPosition;
        readEventsData.count = count;
        readEventsData.batchCounter = batchCounter;
        readEventsData.events = events;
        self.__StartReadEventsBackwardAsync(readEventsData, onSuccess, onFailed);

    def __StartReadEventsBackwardAsync(self,readEventsData, onSuccess, onFailed):
        if readEventsData.batchCounter<readEventsData.count:

            if readEventsData.batchCounter+self.__readBatchSize>readEventsData.count:
                readEventsData.eventsCountInCurrentBatch = readEventsData.count - readEventsData.batchCounter;
            else :
                readEventsData.eventsCountInCurrentBatch = self.__readBatchSize;
            url = self.__baseUrl+"/streams/"+readEventsData.streamId+"/range/"+str(readEventsData.startPosition+readEventsData.batchCounter)+"/"+str(readEventsData.eventsCountInCurrentBatch);

            readEventsData.batchCounter+=self.__readBatchSize
            SendAsync(url, "GET", self.__headers, None, lambda x: self.__ReadStreamEventsBackwardCallback(x, readEventsData, onSuccess, onFailed))

        else:
            onSuccess(EventsAnswer(readEventsData.events))

    def __ReadStreamEventsBackwardCallback(self, response, readEventsData, onSuccess, onFailed):
        if response.code!=200:
            onFailed(FailedAnswer(response.code,"Error occur while reading batch: "+response.error.message))
            return
        try:
            response = json.loads(response.body);
            if len(response['entries'])==0:
                if len(readEventsData.events)!=0:
                    onSuccess(readEventsData.events)
                    return;
                url = self.__baseUrl+"/streams/"+readEventsData.streamId;
                SendAsync(url ,"GET", self.__headers, None, lambda x: self.__OnReadEventsFristResponseEntriesEmpty(x,readEventsData,onSuccess,onFailed))
                return


            batchEvents = []
            for uri in response['entries']:
                url = uri['links']
                for ur in url:
                    try:
                        if ur["type"] == "application/json":
                            url = ur['uri'];
                            SendAsync(url, "GET", self.__headers, None, lambda x: self.__EventReadCallback(x,readEventsData,batchEvents, onSuccess, onFailed ))
                            break;
                    except:
                        continue;
        except httpclient.HTTPError, e:
            onFailed(FailedAnswer(response.code,"Error occur while process batch: "+response.error.message))
            return

    def __OnReadEventsFristResponseEntriesEmpty(self, response,readEventsData, onSuccess, onFailed):
        if response.code!=200:
            onFailed(FailedAnswer(response.code,"Error occur while reading first page: "+response.error.message))
            return
        response = json.loads(response.body);
        if len(response['entries'])==0:
            onSuccess(readEventsData.events)
            return;
        lastEventNumber = int(response["entries"][0]["id"].split('/')[-1])
        if readEventsData.startPosition - lastEventNumber > readEventsData.count:
            onSuccess(readEventsData.events)
            return;
        readEventsData.count =  readEventsData.count + lastEventNumber - readEventsData.startPosition;
        readEventsData.startPosition = lastEventNumber;
        readEventsData.batchCounter =0
        self.__StartReadEventsBackwardAsync(readEventsData, onSuccess, onFailed)

    def __EventReadCallback(self, response, readEventsData,batchEvents, onSuccess, onFailed ):
        try:
            batchEvents.append(json.loads(response.body))
            if len(batchEvents)==readEventsData.eventsCountInCurrentBatch:
                batchEvents = sorted(batchEvents, key=lambda ev: ev['eventNumber'])
                for i in range(len(batchEvents)):
                    readEventsData.events.append(batchEvents[i])
                self.__StartReadEventsBackwardAsync(readEventsData, onSuccess, onFailed)
        except:
            onFailed(FailedAnswer(response.code,"Error occure while reading event: "+response.error.message))

##########################################

    def ReadStreamEventsForwardAsync(self, streamId, startPosition, count,  onSuccess, onFailed):
        ReadStreamEventsBackwardAsync(streamId, startPosition+count, count, lambda x: self.__ReadStreamEventsForwardAsyncCallback(x, onSuccess), onFailed);
    def __ReadStreamEventsForwardAsyncCallback(self, response, onSuccess):
        onSuccess(EventsAnswer(list(reversed(response.events))))

#############################################



    def ReadAllEventsBackwardAsync(self,preparePosition, commitPosition, count, onSuccess, onFailed):
        Ensure.IsFunction(onSuccess, "onSuccess")
        Ensure.IsFunction(onFailed, "onFailed")
        Ensure.IsNumber(preparePosition, "preparePosition")
        Ensure.IsNumber(commitPosition, "commitPosition")
        Ensure.IsPositiveNumber(count, "count")

        hexStartPosition = self.__ConvrtToHex16(preparePosition) + self.__ConvrtToHex16(commitPosition);
        url = self.__baseUrl+"/streams/$all/before/"+hexStartPosition+"/"+str(count);
        SendAsync(url, "GET", self.__headers, None, lambda response: self.__ReadAllEventsAsyncCallback(response, onSuccess, onFailed))

    def ReadAllEventsForwardAsync(self,preparePosition, commitPosition, count, onSuccess, onFailed):
        Ensure.IsFunction(onSuccess, "onSuccess")
        Ensure.IsFunction(onFailed, "onFailed")
        Ensure.IsNumber(preparePosition, "preparePosition")
        Ensure.IsNumber(commitPosition, "commitPosition")
        Ensure.IsPositiveNumber(count, "count")

        hexStartPosition = self.__ConvrtToHex16(preparePosition) + self.__ConvrtToHex16(commitPosition);
        url = self.__baseUrl+"/streams/$all/after/"+hexStartPosition+"/"+str(count);
        SendAsync(url, "GET", self.__headers, None, lambda response: self.__ReadAllEventsAsyncCallback(response, onSuccess, onFailed))
##
##    def __StartReadAllEventsBackwardAsync():


    def __ReadAllEventsAsyncCallback(self, response, onSuccess, onFailed):
        if response.code!=200:
            onFailed(FailedAnswer(response.code,"Error occur while reading links: "+response.error.message))
        readLine = response.body;
        try:
            body = json.loads(readLine);
        except:
            raise;
        newPreparePosition = self.__GetPreparePosition(body['links'][3]["uri"]);
        newCommitPosition = self.__GetCommitPosition(body['links'][3]["uri"])
        events = [];
        eventsCount=0
        try:
            if body['entries'] == []:
                onSuccess(AllEventsAnswer("", -1, -1))
                return;
            for uri in body['entries']:
                url = uri['links']
                for ur in url:
                    try:
                        if ur["type"] == "application/json":
                            url = ur['uri'];
                            SendAsync(url, "GET", self.__headers, None,lambda x: self.__ReadAllEvents(x, events,eventsCount, onSuccess, onFailed, newPreparePosition, newCommitPosition))
                            break;
                    except:
                        continue;
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