from libs import *

class AppendToStreamTest(unittest.TestCase):
    client = ClientAPI();


    def test_appent_events_to_stream(self):
        stream_id = "AppendToStreamTest_test_appent_events_to_stream_stream_id"
        try:
            self.client.create_stream(stream_id,"")
            write_events_count = 1234;
            events = []
            for i in range(write_events_count):
                event_id = stream_id+"_data_"+str(i);
                events.append(Event(event_id,""))
            self.client.append_to_stream(stream_id, events)
            self.assertTrue(True)
        except:
            self.assertTrue(False)


    def test_appent_one_event_to_stream(self):
        stream_id = "AppendToStreamTest_test_appent_one_event_to_stream_stream_id"
        try:
            self.client.create_stream(stream_id,"")
            event_id = stream_id+"_data_event_id"
            self.client.append_to_stream(stream_id, Event(event_id,""))
            self.assertTrue(True)
        except:
            self.assertTrue(False)