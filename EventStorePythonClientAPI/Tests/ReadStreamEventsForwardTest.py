from libs import *

class ReadStreamEventsForwardTest(unittest.TestCase):
    client = ClientAPI()

    def test_read_more_than_one_batch_from_start(self):
        stream_id = "ReadStreamEventsForwardTest_test_read_more_than_one_batch_from_start_stream_id"
        try:
            self.client.create_stream(stream_id,"")
            write_events_count = 1234
            write_events = []
            for i in range(write_events_count):
                event_id = stream_id+"_data_"+str(i)
                write_events.append(WriteEvent(event_id,""))
            self.client.append_to_stream(stream_id, write_events)
            read_events_count = 30
            start_position = 0
            read_events = self.client.read_stream_events_forward(stream_id, start_position, read_events_count)
            for i in range(read_events_count-1):
                self.assertEqual(write_events[i].data, read_events[i+1].data)
            self.assertTrue(True)
        except:
            self.assertTrue(False)


    def test_read_less_than_one_batch_from_start(self):
        stream_id = "ReadStreamEventsForwardTest_test_read_less_than_one_batch_from_start_stream_id"
        try:
            self.client.create_stream(stream_id,"")
            write_events_count = 1234
            write_events = []
            for i in range(write_events_count):
                event_id = stream_id+"_data_"+str(i)
                write_events.append(WriteEvent(event_id,""))
            self.client.append_to_stream(stream_id, write_events)
            read_events_count = 5
            start_position = 0
            read_events = self.client.read_stream_events_forward(stream_id, start_position, read_events_count)
            for i in range(read_events_count-1):
                self.assertEqual(write_events[i].data, read_events[i+1].data)
            self.assertTrue(True)
        except:
            self.assertTrue(False)


    def test_read_events_from_random_position(self):
        stream_id = "ReadStreamEventsForwardTest_test_read_events_from_random_position_position_stream_id"
        try:
            self.client.create_stream(stream_id,"")
            write_events_count = 1234
            write_events = []
            for i in range(write_events_count):
                event_id = stream_id+"_data_"+str(i)
                write_events.append(WriteEvent(event_id,""))
            self.client.append_to_stream(stream_id, write_events)
            read_events_count = 46
            offset = 123
            start_position = write_events_count - offset
            read_events = self.client.read_stream_events_forward(stream_id,start_position, read_events_count)
            for i in range(read_events_count):
                self.assertEqual(write_events[start_position+i-1].data, read_events[i].data)
            self.assertTrue(True)
        except:
            self.assertTrue(False)


    def test_read_more_than_one_batch_from_end(self):
        stream_id = "ReadStreamEventsForwardTest_test_read_more_than_one_batch_from_end_stream_id"
        try:
            self.client.create_stream(stream_id,"")
            write_events_count = 1234
            write_events = []
            for i in range(write_events_count):
                event_id = stream_id+"_data_"+str(i)
                write_events.append(WriteEvent(event_id,""))
            self.client.append_to_stream(stream_id, write_events)
            read_events_count = 100
            start_position = 1200
            read_events = self.client.read_stream_events_forward(stream_id, start_position, read_events_count)
            for i in range(len(read_events)):
                self.assertEqual(write_events[start_position + i -1].data, read_events[i].data)
            self.assertTrue(True)
        except:
            self.assertTrue(False)