from libs import *

class ReadStreamEventsBackwardTest(unittest.TestCase):
    client = ClientAPI()

    def test_read_more_than_one_batch_from_last_position(self):
        stream_id = "ReadStreamEventsBackwardTest_test_read_more_than_one_batch_from_last_position_stream_id"
        try:
            self.client.create_stream(stream_id,"")
            write_events_count = 1234
            write_events = []
            for i in range(write_events_count):
                event_id = stream_id+"_data_"+str(i)
                write_events.append(WriteEvent(event_id,""))
            self.client.append_to_stream(stream_id, write_events)
            read_events_count = 30
            read_events = self.client.read_stream_events_backward(stream_id,write_events_count, read_events_count)
            for i in range(read_events_count):
                self.assertEqual(write_events[write_events_count-i-1].data, read_events[i].data)
            self.assertTrue(True)
        except:
            self.assertTrue(False)


    def test_read_less_than_one_batch_from_last_position(self):
        stream_id = "ReadStreamEventsBackwardTest_test_read_less_than_one_batch_from_last_position_stream_id"
        try:
            self.client.create_stream(stream_id,"")
            write_events_count = 1234
            write_events = []
            for i in range(write_events_count):
                event_id = stream_id+"_data_"+str(i)
                write_events.append(WriteEvent(event_id,""))
            self.client.append_to_stream(stream_id, write_events)
            read_events_count = 5
            read_events = self.client.read_stream_events_backward(stream_id,write_events_count, read_events_count)
            for i in range(read_events_count):
                self.assertEqual(write_events[write_events_count-i-1].data, read_events[i].data)
            self.assertTrue(True)
        except:
            self.assertTrue(False)


    def test_read_less_than_one_batch_from_random_position(self):
        stream_id = "ReadStreamEventsBackwardTest_test_read_less_than_one_batch_from_random_position_stream_id"
        try:
            self.client.create_stream(stream_id,"")
            write_events_count = 1234
            write_events = []
            for i in range(write_events_count):
                event_id = stream_id+"_data_"+str(i)
                write_events.append(WriteEvent(event_id,""))
            self.client.append_to_stream(stream_id, write_events)
            read_events_count = 5
            offset = 123
            start_position = write_events_count - offset
            read_events = self.client.read_stream_events_backward(stream_id,start_position, read_events_count)
            for i in range(read_events_count):
                self.assertEqual(write_events[start_position-i-1].data, read_events[i].data)
            self.assertTrue(True)
        except:
            self.assertTrue(False)


    def test_read_more_than_one_batch_from_random_position(self):
        stream_id = "ReadStreamEventsBackwardTest_test_read_more_than_one_batch_from_random_position_stream_id"
        try:
            self.client.create_stream(stream_id,"")
            write_events_count = 1234
            write_events = []
            for i in range(write_events_count):
                event_id = stream_id+"_data_"+str(i)
                write_events.append(WriteEvent(event_id,""))
            self.client.append_to_stream(stream_id, write_events)
            read_events_count = 50
            offset = 123
            start_position = write_events_count - offset
            read_events = self.client.read_stream_events_backward(stream_id,start_position, read_events_count)
            for i in range(read_events_count):
                self.assertEqual(write_events[start_position-i-1].data, read_events[i].data)
            self.assertTrue(True)
        except:
            self.assertTrue(False)


    def test_read_couple_events_from_not_exist_position(self):
        stream_id = "ReadStreamEventsBackwardTest_test_read_couple_events_from_not_exist_position_stream_id"
        try:
            self.client.create_stream(stream_id,"")
            write_events_count = 1234
            write_events = []
            for i in range(write_events_count):
                event_id = stream_id+"_data_"+str(i)
                write_events.append(WriteEvent(event_id,""))
            self.client.append_to_stream(stream_id, write_events)
            read_events_count = 50
            offset = -123
            start_position = write_events_count - offset
            read_events = self.client.read_stream_events_backward(stream_id,start_position, read_events_count)
            self.assertEqual(read_events, [])
        except:
            self.assertTrue(False)


    def test_read_many_events_from_not_exist_position(self):
        stream_id = "ReadStreamEventsBackwardTest_test_read_many_events_from_not_exist_position_stream_id"
        try:
            self.client.create_stream(stream_id,"")
            write_events_count = 1234
            write_events = []
            for i in range(write_events_count):
                event_id = stream_id+"_data_"+str(i)
                write_events.append(WriteEvent(event_id,""))
            self.client.append_to_stream(stream_id, write_events)
            read_events_count = 100
            offset = -50
            start_position = write_events_count - offset
            read_events = self.client.read_stream_events_backward(stream_id,start_position, read_events_count)
            for i in range(read_events_count+offset):
                self.assertEqual(write_events[write_events_count-i-1].data, read_events[i].data)
        except:
            self.assertTrue(False)


    def test_read_many_events_from_start(self):
        stream_id = "ReadStreamEventsBackwardTest_test_read_many_events_from_start_stream_id"
        try:
            self.client.create_stream(stream_id,"")
            write_events_count = 200
            write_events = []
            for i in range(write_events_count):
                event_id = stream_id+"_data_"+str(i)
                write_events.append(WriteEvent(event_id,""))
            self.client.append_to_stream(stream_id, write_events)
            read_events_count = 100
            start_position = 10
            read_events = self.client.read_stream_events_backward(stream_id,start_position, read_events_count)
            for i in range(start_position-1):
                self.assertEqual(read_events[start_position-i-1].data, write_events[i].data)
        except:
            self.assertTrue(False)