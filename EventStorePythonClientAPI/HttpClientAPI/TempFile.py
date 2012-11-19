import EventStoreClient;
from  Event import Event;
from threading import Thread
from threading import Semaphore
import uuid
import datetime;
import time


class EventStoreWriter:
    def __init__(self, streamId, eventsCount=1000, threads=60, writeSize = 1):
        self.eventsCount = eventsCount
        self.threads = threads
        self.streamId = streamId;
        self.writeSize = writeSize;

    def WriteEvent(self):
        for i in range(self.writeSize):
            res = client.AppendToStream(streamId,Event("data"+str(uuid.uuid4()), ""));
            print(res.reason)
        self.counter+=self.writeSize;
##        semaphore = Semaphore()
##        semaphore.acquire();
##        if self.counter<self.eventsCount:
##            self.counter+=self.writeSize;
##            semaphore.release();
##            self.WriteEvent();
##        else:
##            self.endTime = datetime.datetime.now()

    def Start(self):
        self.counter = 0;
        self.startTime = datetime.datetime.now()
        for i in range(self.threads):
            t = Thread(target=self.WriteEvent)
            t.start()
        time.sleep(1);
        print(str(self.counter))

client = EventStoreClient.EventStoreClient()
streamId = "SomeStream2"
responce = client.CreateStream(streamId,"")
print(responce.reason)
##eventId = str(uuid.uuid4())
##responce = client.AppendToStream(streamId, Event("data","",eventId))
##print(responce.reason)
esw = EventStoreWriter(streamId=streamId)
esw.Start();