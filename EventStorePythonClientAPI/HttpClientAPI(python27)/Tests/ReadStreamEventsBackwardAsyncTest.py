from libs import *

class ReadStreamEventsBackwardTest(unittest.TestCase):
    __client = ClientAPI();
    __callbackResult = None

    def test_read_more_than_one_batch(self):
        streamId = "ReadStreamEventsBackwardTest_test_read_more_than_one_batch_stream_id"
        self.__client.CreateStream(streamId,"",lambda s: self.__Success(s), lambda s: self.__Failed(s))
        self.assertTrue(self.__callbackResult)
        writeEventsCount = 1234;
        events = []
        for i in range(writeEventsCount):
            eventId = streamId+"_data_"+str(i);
            events.append(Event(eventId,""))
        self.__client.AppendToStream(streamId, events,lambda s: self.__Success(s), lambda s: self.__Failed(s))
        self.assertTrue(self.__callbackResult)

        readEventsCount = 110;
        offset = 123
        startPosition = writeEventsCount-offset;
        i = startPosition
        expectedEvents =[]
        while i>startPosition - readEventsCount:
            eventId = streamId+"_data_"+str(i);
            expectedEvents.append(Event(eventId,""))
            i-=1
        self.__client.ReadStreamEventsBackward(streamId, startPosition, readEventsCount,  lambda s: self.__ReadSuccess(s), lambda s: self.__Failed(s))
        self.assertTrue(self.__callbackResult)
        for i in range(len(expectedEvents)):
            self.assertEqual(expectedEvents[i].data, self.__readEvents.events[i]['data'])
            self.assertEqual(expectedEvents[i].metadata, self.__readEvents.events[i]['metadata'])
            self.assertEqual(expectedEvents[i].eventId, self.__readEvents.events[i]['eventId'])

    def __Success(self, response):
        self.__callbackResult = True;

    def __Failed(self, response):
        self.__callbackResult = False;

    def __ReadSuccess(self, response):
        self.__readEvents = response;



if __name__ == '__main__':
    os.startfile('D:\\apps\\EventStore\\bin\\eventstore\\debug\\anycpu\\EventStore.SingleNode.exe');
    time.sleep(3)
    unittest.main()