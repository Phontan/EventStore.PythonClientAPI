from libs import *

class AppendToStreamTest(unittest.TestCase):
    __client = ClientAPI();


    def test_appent_events_to_stream(self):
        streamId = "AppendToStreamTest_test_appent_events_to_stream_stream_id"
        try:
            self.__client.CreateStream(streamId,"")
            writeEventsCount = 1234;
            events = []
            for i in range(writeEventsCount):
                eventId = streamId+"_data_"+str(i);
                events.append(Event(eventId,""))
            self.__client.AppendToStream(streamId, events)
            self.assertTrue(True)
        except:
            self.assertTrue(False)


    def test_appent_one_event_to_stream(self):
        streamId = "AppendToStreamTest_test_appent_one_event_to_stream_stream_id"
        try:
            self.__client.CreateStream(streamId,"")
            eventId = streamId+"_data_eventId"
            self.__client.AppendToStream(streamId, Event(eventId,""))
            self.assertTrue(True)
        except:
            self.assertTrue(False)