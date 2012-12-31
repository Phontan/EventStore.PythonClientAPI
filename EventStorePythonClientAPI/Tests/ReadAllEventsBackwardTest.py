from libs import *

class ReadAllEventsBackwardTest(unittest.TestCase):
    client = ClientAPI()

    def test_read_more_than_one_batch_from_last_position(self):
        stream_id = "ReadAllEventsBackwardTest_test_read_more_than_one_batch_from_last_position_stream_id"
        try:
            self.client.create_stream(stream_id,"")
            write_events_count = 100
            write_events = []
            for i in range(write_events_count):
                event_id = stream_id+"_data_"+str(i)
                write_events.append(WriteEvent(event_id,""))
            self.client.append_to_stream(stream_id, write_events)
            read_events_count = 30
            read_events = self.client.read_all_events_backward(-1,-1,read_events_count)
            for i in range(read_events_count):
                self.assertEqual(write_events[write_events_count-i-1].data, read_events.events[i].data)
            self.assertTrue(True)
        except:
            self.assertTrue(False)