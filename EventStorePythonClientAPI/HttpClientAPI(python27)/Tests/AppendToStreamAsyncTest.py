from libs import *

class AppendToStreamTest(unittest.TestCase):
    __client = ClientAPI();
    __callbackResult = None


    def test_appent_events_to_stream(self):
        streamId = "AppendToStreamTest_test_appent_events_to_stream_stream_id"
        self.__client.CreateStream(streamId,"",lambda s: self.__Success(s), lambda s: self.__Failed(s))
        self.assertTrue(self.__callbackResult)
        writeEventsCount = 1234;
        events = []
        for i in range(writeEventsCount):
            eventId = streamId+"_data_"+str(i);
            events.append(Event(eventId,""))
        self.__client.AppendToStream(streamId, events,lambda s: self.__Success(s), lambda s: self.__Failed(s))
        self.assertTrue(self.__callbackResult)


    def test_appent_one_event_to_stream(self):
        streamId = "AppendToStreamTest_test_appent_one_event_to_stream_stream_id"
        self.__client.CreateStream(streamId,"",lambda s: self.__Success(s), lambda s: self.__Failed(s))
        self.assertTrue(self.__callbackResult)
        eventId = streamId+"_data_eventId"
        self.__client.AppendToStream(streamId, Event(eventId,""),lambda s: self.__Success(s), lambda s: self.__Failed(s))
        self.assertTrue(self.__callbackResult)


    def __Success(self, response):
        self.__callbackResult = True;

    def __Failed(self, response):
        self.__callbackResult = False;


if __name__ == '__main__':
    os.startfile('D:\\apps\\EventStore\\bin\\eventstore\\debug\\anycpu\\EventStore.SingleNode.exe');
    time.sleep(3)
    unittest.main()