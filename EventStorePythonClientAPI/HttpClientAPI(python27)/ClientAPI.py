from Event import *
from ClientJsonSerelizationOption import *
import sys, os
sys.path.append(os.path.dirname(__file__)+"\\libs");
from bodyLibs import *
from answerLibs import *
from AsyncRequestSender import *
import Ensure
from threading import*
from ReadEventsData import *
from SyncResponse import *
from collections import deque
import time
import httplib

class ClientAPI:
    def __init__(self, ipAddress="http://127.0.0.1", port = 2113):
        self.__baseUrl = ipAddress+':'+str(port);
        self.__headers = {"content-type" : "application/json","accept" :  "application/json","extensions" : "json"};
        self.__readBatchSize = 20
        self.__tornadoHttpSender = TornadoHttpSender();


    def Resume(self):
        tornado.ioloop.IOLoop.instance().stop();

    def Wait(self):
        tornado.ioloop.IOLoop.instance().start()


    def __SyncSuccess(self, response):
        self.Resume()
        response = SyncResponse(True, response);
        return response
    def __SyncFailed(self, response):
        self.Resume()
        response = SyncResponse(False, response);
        return response


################################################################################


    def CreateStream(self, streamId, metadata):
        queue = deque();
        onSuccess = lambda x:  queue.append(self.__SyncSuccess(x))
        onFailed = lambda x:  queue.append(self.__SyncFailed(x))
        self.__StartCreateStream(streamId, metadata, onSuccess, onFailed)
        self.Wait()
        result = queue.popleft()
        if result.success:
            return
        else:
            raise result.response

    def CreateStreamAsync(self, streamId, metadata, onSuccess, onFailed):
        self.__StartCreateStream(streamId, metadata, onSuccess, onFailed)


    def __StartCreateStream(self, streamId, metadata, onSuccess, onFailed):
        Ensure.IsNotEmptyString(streamId, "streamId")
        Ensure.IsString(metadata, "metadata")
        Ensure.IsFunction(onSuccess, "onSuccess")
        Ensure.IsFunction(onFailed, "onFailed")
        try:
            body = ToJson(CreateStreamRequestBody(streamId, metadata))
        except:
            raise;
        url = self.__baseUrl+"/streams";
        self.__tornadoHttpSender.SendAsync(url, "POST", self.__headers, body,lambda x: self.__CreateStreamCallback(x, onSuccess, onFailed))

    def __CreateStreamCallback(self, response, onSuccess,onFailed):
        if response.code==201:
            onSuccess(response);
        else:
            onFailed(FailedAnswer(response.code,response.error.message));



#################################################################



    def DeleteStream(self, streamId, expectedVersion=-2):
        queue = deque();
        onSuccess = lambda x:  queue.append(self.__SyncSuccess(x))
        onFailed = lambda x:  queue.append(self.__SyncFailed(x))
        self.__StartDeleteStream(streamId , onSuccess, onFailed, expectedVersion);
        self.Wait()
        result = queue.popleft()
        if result.success:
            return
        else:
            raise result.response

    def DeleteStreamAsync(self,streamId , onSuccess, onFailed,expectedVersion=-2):
        self.__StartDeleteStream(streamId , onSuccess, onFailed, expectedVersion)


    def __StartDeleteStream(self,streamId , onSuccess, onFailed,expectedVersion):
        Ensure.IsNotEmptyString(streamId, "streamId")
        Ensure.IsFunction(onSuccess, "onSuccess")
        Ensure.IsFunction(onFailed, "onFailed")
        Ensure.IsNumber(expectedVersion, "expectedVersion")

        url = self.__baseUrl+"/streams/"+streamId;
        try:
            body = ToJson(DeleteStreamRequestBody(expectedVersion));
        except:
            raise;
        self.__tornadoHttpSender.SendAsync(url, "DELETE", self.__headers, body, lambda x: self.__DeleteStreamCallback(x, onSuccess, onFailed));

    def __DeleteStreamCallback(self, response, onSuccess,onFailed):
        if response.code==204:
            onSuccess(response);
        else:
            onFailed(FailedAnswer(response.code,response.error.message));


#######################################################



    def AppendToStream(self,streamId, events,expectedVersion=-2):
        queue = deque();
        onSuccess = lambda x:  queue.append(self.__SyncSuccess(x))
        onFailed = lambda x:  queue.append(self.__SyncFailed(x))
        self.__StartAppendToStream(streamId, events, onSuccess, onFailed, expectedVersion)
        self.Wait()
        result = queue.popleft()
        if result.success:
            return
        else:
            raise result.response

    def AppendToStreamAsync(self,streamId, events, onSuccess, onFailed, expectedVersion=-2):
        self.__StartAppendToStream(streamId, events, onSuccess, onFailed, expectedVersion)


    def __StartAppendToStream(self,streamId, events, onSuccess, onFailed, expectedVersion):
        Ensure.IsNotEmptyString(streamId, "streamId")
        Ensure.IsFunction(onSuccess, "onSuccess")
        Ensure.IsFunction(onFailed, "onFailed")
        Ensure.IsNumber(expectedVersion, "expectedVersion")

        if(type(list())!= type(events)):
            newData = list();
            newData.append(events);
            events = newData;
        try:
            body = ToJson(AppendToStreamRequestBody(expectedVersion, events))
        except:
            raise;
        url = self.__baseUrl+"/streams/"+streamId
        self.__tornadoHttpSender.SendAsync(url,"POST", self.__headers, body, lambda x: self.__AppendToStreamCallback(x, onSuccess, onFailed))


    def __AppendToStreamCallback(self, response, onSuccess,onFailed):
        if response.code==201:
            onSuccess(response);
        else:
            onFailed(FailedAnswer(response.code,response.error.message))



###########################################################


    def ReadEvent(self,streamId , eventNumber, resolve=1):
        queue = deque();
        onSuccess = lambda x:  queue.append(self.__SyncSuccess(x))
        onFailed = lambda x:  queue.append(self.__SyncFailed(x))
        self.__StartReadEvent(streamId , eventNumber,  onSuccess, onFailed, resolve=1)
        self.Wait()
        result = queue.popleft()
        if result.success:
            return result.response
        else:
            raise result.response

    def ReadEventAsync(self,streamId , eventNumber,  onSuccess, onFailed, resolve=1):
        self.__StartReadEvent(streamId , eventNumber,  onSuccess, onFailed, resolve=1)


    def __StartReadEvent(self,streamId , eventNumber,  onSuccess, onFailed, resolve=1):
        Ensure.IsNotEmptyString(streamId, "streamId")
        Ensure.IsFunction(onSuccess, "onSuccess")
        Ensure.IsFunction(onFailed, "onFailed")
        Ensure.IsNotNegativeNumber(eventNumber, "eventNumber")
        if resolve: resolve = "yes"
        else: resolve="no"
        url = self.__baseUrl+"/streams/"+streamId+"/event/"+str(eventNumber)+"?resolve="+resolve
        self.__tornadoHttpSender.SendAsync(url, "GET", self.__headers, None, lambda x: self.__ReadEventCallback(x, onSuccess, onFailed))

    def __ReadEventCallback(self, response, onSuccess, onFailed):
        if response.code!=200:
            onFailed(FailedAnswer(response.code,response.error.message))
            return;
        responseContent = response.body
        try:
            event = json.loads(responseContent)
            onSuccess(event)
        except:
            raise;



################################################ Read Stream Events Backward ###############



    def ReadStreamEventsBackwardAsync(self, streamId, startPosition, count,  onSuccess, onFailed):
        self.__StartReadStreamEventsBackward(streamId, startPosition, count,  onSuccess, onFailed);

    def ReadStreamEventsBackward(self, streamId, startPosition, count):
        queue = deque();
        onSuccess = lambda x:  queue.append(self.__SyncSuccess(x))
        onFailed = lambda x:  queue.append(self.__SyncFailed(x))
        self.__StartReadStreamEventsBackward(streamId, startPosition, count,  onSuccess, onFailed);
        self.Wait()
        result = queue.popleft()
        if result.success:
            return result.response
        else:
            raise result.response


    def __StartReadStreamEventsBackward(self, streamId, startPosition, count,  onSuccess, onFailed):
        Ensure.IsNotEmptyString(streamId, "streamId")
        Ensure.IsFunction(onSuccess, "onSuccess")
        Ensure.IsFunction(onFailed, "onFailed")
        Ensure.IsNotNegativeNumber(startPosition, "startPosition")
        Ensure.IsPositiveNumber(count, "count")
        events = []
        batchCounter=0
        params = ReadEventsData;
        params.streamId = streamId;
        params.startPosition = startPosition;
        params.count = count;
        params.batchCounter = batchCounter;
        params.events = events;
        self.__ReadBatchEventsBackward(params, onSuccess, onFailed);

    def __ReadBatchEventsBackward(self,params, onSuccess, onFailed, eventsCount=None):
        if eventsCount!=None and  eventsCount<self.__readBatchSize:
            onSuccess(params.events)
            return
        if params.batchCounter<params.count:
            if params.batchCounter + self.__readBatchSize>params.count:
                params.batchLength = params.count - params.batchCounter;
            else :
                params.batchLength = self.__readBatchSize;
            url = self.__baseUrl + "/streams/{0}/range/{1}/{2}".format( \
                  params.streamId, \
                  str(params.startPosition-params.batchCounter), \
                  str(params.batchLength));
            params.batchCounter+=self.__readBatchSize
            self.__tornadoHttpSender.SendAsync(url, "GET", self.__headers, None, lambda x: self.__ReadStreamEventsBackwardCallback(x, params, onSuccess, onFailed))
        else:
            onSuccess(params.events)

    def __ReadStreamEventsBackwardCallback(self, response, params, onSuccess, onFailed):
        if response.code!=200:
            onFailed(FailedAnswer(response.code,"Error occur while reading batch: "+response.error.message))
            return
        try:
            response = json.loads(response.body);
            if len(response['entries'])==0:
                if len(params.events)!=0:
                    onSuccess(params.events)
                    return;
                url = self.__baseUrl+"/streams/"+params.streamId;
                self.__tornadoHttpSender.SendAsync(url ,"GET", self.__headers, None, lambda x: self.__OnReadEventsFristResponseEntriesEmpty(x,params,onSuccess,onFailed))
                return
            batchEvents = []
            for uri in response['entries']:
                url = uri['links']
                for ur in url:
                    try:
                        if ur["type"] == "application/json":
                            url = ur['uri'];
                            self.__tornadoHttpSender.SendAsync(url, "GET", self.__headers, None, lambda x: self.__EventReadCallback(x,params,batchEvents, onSuccess, onFailed,len(response['entries'])))
                            break;
                    except:
                        continue;
        except httpclient.HTTPError, e:
            onFailed(FailedAnswer(response.code,"Error occur while process batch: "+response.error.message))
            return

    def __OnReadEventsFristResponseEntriesEmpty(self, response,params, onSuccess, onFailed):
        if response.code!=200:
            onFailed(FailedAnswer(response.code,"Error occur while reading first page: "+response.error.message))
            return
        response = json.loads(response.body);
        if len(response['entries'])==0:
            onSuccess(params.events)
            return;
        lastEventNumber = int(response["entries"][0]["id"].split('/')[-1])
        if params.startPosition - lastEventNumber > params.count:
            onSuccess(params.events)
            return;
        params.count =  params.count + lastEventNumber - params.startPosition;
        params.startPosition = lastEventNumber;
        params.batchCounter =0;
        self.__ReadBatchEventsBackward(params, onSuccess, onFailed)

    def __EventReadCallback(self, response, params,batchEvents, onSuccess, onFailed, eventsCount):
        if response.code !=200:
            onFailed(FailedAnswer(response.code,response.error.message))
        try:
            batchEvents.append(json.loads(response.body))
            if len(batchEvents)==eventsCount:
                batchEvents = sorted(batchEvents, key=lambda ev: ev['eventNumber'], reverse=True)
                for i in range(len(batchEvents)):
                    params.events.append(batchEvents[i])
                self.__ReadBatchEventsBackward(params, onSuccess, onFailed, eventsCount)
        except:
            onFailed(FailedAnswer(response.code,"Error occure while reading event: "+response.error.message))



##########################################



    def ReadStreamEventsForwardAsync(self, streamId, startPosition, count,  onSuccess, onFailed):
        self.__StartReadStreamEventsBackward(streamId, startPosition+count, count, lambda x: self.__ReadStreamEventsForwardCallback(x, onSuccess), onFailed)

    def ReadStreamEventsForward(self, streamId, startPosition, count):
        queue = deque();
        onSuccess = lambda x:  queue.append(self.__SyncSuccess(list(reversed(x))))
        onFailed = lambda x:  queue.append(self.__SyncFailed(x))
        self.__StartReadStreamEventsBackward(streamId, startPosition+count-1, count,  onSuccess, onFailed);
        self.Wait()
        result = queue.popleft()
        if result.success:
            return result.response
        else:
            raise result.response


############################################# READ ALL EVENTS BACKWARD #####################################################################


    def ReadAllEventsBackward(self,preparePosition, commitPosition, count):
        queue = deque();
        onSuccess = lambda x:  queue.append(self.__SyncSuccess(x))
        onFailed = lambda x:  queue.append(self.__SyncFailed(x))
        self.__StartReadAllEventsBackward(preparePosition, commitPosition, count, onSuccess, onFailed);
        self.Wait()
        result = queue.popleft()
        if result.success:
            return result.response
        else:
            raise result.response


    def ReadAllEventsBackwardAsync(self,preparePosition, commitPosition, count, onSuccess, onFailed):
        self.__StartReadAllEventsBackward(preparePosition, commitPosition, count, onSuccess, onFailed)


    def __StartReadAllEventsBackward(self,preparePosition, commitPosition, count, onSuccess, onFailed):
        Ensure.IsFunction(onSuccess, "onSuccess")
        Ensure.IsFunction(onFailed, "onFailed")
        Ensure.IsNumber(preparePosition, "preparePosition")
        Ensure.IsNumber(commitPosition, "commitPosition")
        Ensure.IsPositiveNumber(count, "count")
        events = []
        batchCounter=0
        params = ReadEventsData
        params.preparePosition = preparePosition
        params.commitPosition = commitPosition
        params.count = count
        params.batchCounter = batchCounter
        params.events = events
        self.__ReadBatchAllEventsBackward(params, onSuccess, onFailed)

    def __ReadBatchAllEventsBackward(self,params, onSuccess, onFailed, eventsCount=None):
        if eventsCount!=None and  eventsCount<self.__readBatchSize:
            onSuccess(AllEventsAnswer(params.events, params.preparePosition, params.commitPosition))
            return
        if params.batchCounter<params.count:
            hexStartPosition = self.__ConvrtToHex16(params.preparePosition) + self.__ConvrtToHex16(params.commitPosition);
            if params.batchCounter+self.__readBatchSize>params.count:
                params.batchLength = params.count - params.batchCounter;
            else :
                params.batchLength = self.__readBatchSize;
            url = self.__baseUrl+"/streams/$all/before/"+hexStartPosition+"/"+str(params.batchLength);
            params.batchCounter+=self.__readBatchSize
            self.__tornadoHttpSender.SendAsync(url, "GET", self.__headers, None, lambda x: self.__ReadAllEventsBackwardCallback(x, params, onSuccess, onFailed))
        else:
            onSuccess(AllEventsAnswer(params.events, params.preparePosition, params.commitPosition))

    def __ReadAllEventsBackwardCallback(self, response,params, onSuccess, onFailed):
        if response.code!=200:
            onFailed(FailedAnswer(response.code,"Error occur while reading links: "+response.error.message))
        readLine = response.body;
        try:
            body = json.loads(readLine);
        except:
            raise;
        params.preparePosition = self.__GetPreparePosition(body['links'][4]["uri"]);
        params.commitPosition = self.__GetCommitPosition(body['links'][4]["uri"])

        try:
            if body['entries'] == []:
                if len(params.events)!=0:
                    onSuccess(AllEventsAnswer("", 0,0 ))
                else:
                    self.__StartReadAllEventsBackward(-1,-1,params.count, onSuccess, onFailed)
                return;
            eventsCount=len(body['entries'])
            batchEvents = {}
            urlNumberDictionary = {}
            eventNumber = 0;
            for uri in body['entries']:
                url = uri['links']
                for ur in url:
                    try:
                        if ur["type"] == "application/json":
                            url = ur['uri'];
                            urlNumberDictionary[url] = eventNumber
                            eventNumber+=1
                            self.__tornadoHttpSender.SendAsync(url, "GET", self.__headers, None,lambda x: self.__ReadAllEventsBackward(x, params, batchEvents, onSuccess, onFailed,eventsCount, urlNumberDictionary))
                            break;
                    except:
                        continue;
        except:
            onFailed(FailedAnswer(response.code,response.error.message))
            return;

    def __ReadAllEventsBackward(self, response, params, batchEvents, onSuccess, onFailed,eventsCount, urlNumberDictionary):
        if response.code !=200:
            onFailed(FailedAnswer(response.code,response.error.message))
            return
        try:
            batchEvents[urlNumberDictionary[response.request.url]]=json.loads(response.body)
            if len(batchEvents)==eventsCount:
                i = 0
                while i<eventsCount:
                    params.events.append(batchEvents[i])
                    i+=1
                self.__ReadBatchAllEventsBackward(params, onSuccess, onFailed, eventsCount)
        except:
            onFailed(FailedAnswer(response.code,"Error occure while reading event: "+response.error.message))



############################################# READ ALL EVENTS BACKWARD #####################################################################


    def ReadAllEventsForward(self,preparePosition, commitPosition, count):
        queue = deque();
        onSuccess = lambda x:  queue.append(self.__SyncSuccess(x))
        onFailed = lambda x:  queue.append(self.__SyncFailed(x))
        self.__StartReadAllEventsForward(preparePosition, commitPosition, count, onSuccess, onFailed);
        self.Wait()
        result = queue.popleft()
        if result.success:
            return result.response
        else:
            raise result.response


    def ReadAllEventsForwardAsync(self,preparePosition, commitPosition, count, onSuccess, onFailed):
        self.__StartReadAllEventsForward(preparePosition, commitPosition, count, onSuccess, onFailed)


    def __StartReadAllEventsForward(self,preparePosition, commitPosition, count, onSuccess, onFailed):
        Ensure.IsFunction(onSuccess, "onSuccess")
        Ensure.IsFunction(onFailed, "onFailed")
        Ensure.IsNotNegativeNumber(preparePosition, "preparePosition")
        Ensure.IsNotNegativeNumber(commitPosition, "commitPosition")
        Ensure.IsPositiveNumber(count, "count")
        events = []
        batchCounter=0
        params = ReadEventsData
        params.preparePosition = preparePosition
        params.commitPosition = commitPosition
        params.count = count
        params.batchCounter = batchCounter
        params.events = events
        self.__ReadBatchAllEventsForward(params, onSuccess, onFailed)

    def __ReadBatchAllEventsForward(self,params, onSuccess, onFailed, eventsCount=None):
        if eventsCount!=None and  eventsCount<self.__readBatchSize:
            onSuccess(AllEventsAnswer(params.events, params.preparePosition, params.commitPosition))
            return
        if params.batchCounter<params.count:
            hexStartPosition = self.__ConvrtToHex16(params.preparePosition) + self.__ConvrtToHex16(params.commitPosition);
            if params.batchCounter+self.__readBatchSize>params.count:
                params.batchLength = params.count - params.batchCounter;
            else :
                params.batchLength = self.__readBatchSize;
            url = self.__baseUrl+"/streams/$all/after/"+hexStartPosition+"/"+str(params.batchLength);
            params.batchCounter+=self.__readBatchSize
            self.__tornadoHttpSender.SendAsync(url, "GET", self.__headers, None, lambda x: self.__ReadAllEventsForwardCallback(x, params, onSuccess, onFailed))
        else:
            onSuccess(AllEventsAnswer(params.events, params.preparePosition, params.commitPosition))

    def __ReadAllEventsForwardCallback(self, response,params, onSuccess, onFailed):
        if response.code!=200:
            onFailed(FailedAnswer(response.code,"Error occur while reading links: "+response.error.message))
        readLine = response.body;
        try:
            body = json.loads(readLine);
        except:
            raise;
        params.preparePosition = self.__GetPreparePosition(body['links'][3]["uri"]);
        params.commitPosition = self.__GetCommitPosition(body['links'][3]["uri"])

        try:
            if body['entries'] == []:
                if len(params.events)!=0:
                    onSuccess(AllEventsAnswer("", -1,-1 ))
                else:
                    self.__StartReadAllEventsBackward(0,0,params.count, onSuccess, onFailed)
                return;
            eventsCount=len(body['entries'])
            batchEvents = []
            urlNumberDictionary = {}
            eventNumber = 0;
            for uri in body['entries']:
                url = uri['links']
                for ur in url:
                    try:
                        if ur["type"] == "application/json":
                            url = ur['uri'];
                            urlNumberDictionary[url] = eventNumber
                            eventNumber+=1
                            self.__tornadoHttpSender.SendAsync(url, "GET", self.__headers, None,lambda x: self.__ReadAllEventsForward(x, params, batchEvents, onSuccess, onFailed,eventsCount, urlNumberDictionary))
                            break;
                    except:
                        continue;
        except:
            onFailed(FailedAnswer(response.code,response.error.message))
            return;

    def __ReadAllEventsForward(self, response, params, batchEvents, onSuccess, onFailed,eventsCount, urlNumberDictionary):
        if response.code !=200:
            onFailed(FailedAnswer(response.code,response.error.message))
            return
        try:
            batchEvents.insert(urlNumberDictionary[response.request.url],json.loads(response.body))
            if len(batchEvents)==eventsCount:
                i = eventsCount-1
                while i>=0:
                    params.events.append(batchEvents[i])
                    i-=1
                self.__ReadBatchAllEventsForward(params, onSuccess, onFailed, eventsCount)
        except:
            onFailed(FailedAnswer(response.code,"Error occure while reading event: "+response.error.message))




#################################################################################################################

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