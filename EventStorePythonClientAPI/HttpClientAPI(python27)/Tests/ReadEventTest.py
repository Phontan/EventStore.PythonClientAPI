from libs import *

class ReadEventTest(unittest.TestCase):
    __client = ClientAPI();

    def test_read_event(self):
        streamId = "ReadEventTest_test_read_event_stream_id"
        try:
            self.__client.CreateStream(streamId,"")
            writeEventsCount = 1234;
            events = []
            for i in range(writeEventsCount):
                eventId = streamId+"_data_"+str(i);
                events.append(Event(eventId,""))
            self.__client.AppendToStream(streamId, events)
            event1234 = self.__client.ReadEvent(streamId, 1234)
            self.assertEqual(event1234['data'], events[1233].data)
            event123 = self.__client.ReadEvent(streamId, 123)
            self.assertEqual(event123['data'], events[122].data)
            self.assertTrue(True)
        except:
            self.assertTrue(False)