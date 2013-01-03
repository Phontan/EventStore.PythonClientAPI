from libs import *

class ReadEventTest(unittest.TestCase):
    __client = ClientAPI()

    def test_read_event(self):
        stream_id = "ReadEventTest_test_read_event_stream_id"
        try:
            self.__client.create_stream(stream_id,"")
            write_events_count = 1234
            events = []
            for i in range(write_events_count):
                event_id = stream_id+"_data_"+str(i)
                events.append(WriteEvent(event_id,""))
            self.__client.append_to_stream(stream_id, events)
            event1234 = self.__client.read_event(stream_id, 1234)
            self.assertEqual(event1234.data, events[1233].data)
            event123 = self.__client.read_event(stream_id, 123)
            self.assertEqual(event123.data, events[122].data)
            self.assertTrue(True)
        except:
            self.assertTrue(False)


    def test_read_event_with_empty_spaces_stream(self):
        stream_id = "ReadEventTest_test_read_event_with_empty_spaces_stream stream_id"
        try:
            self.__client.create_stream(stream_id,"")
            write_events_count = 1234
            events = []
            for i in range(write_events_count):
                event_id = stream_id+"_data_"+str(i)
                events.append(WriteEvent(event_id,""))
            self.__client.append_to_stream(stream_id, events)
            event1234 = self.__client.read_event(stream_id, 1234)
            self.assertEqual(event1234.data, events[1233].data)
            event123 = self.__client.read_event(stream_id, 123)
            self.assertEqual(event123.data, events[122].data)
            self.assertTrue(True)
        except:
            self.assertTrue(False)