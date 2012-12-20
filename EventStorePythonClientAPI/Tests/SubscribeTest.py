from libs import *

class SubscribeTest(unittest.TestCase):
    client = ClientAPI()

    def test_subscribe(self):
        try:
            stream_id = "SubscribeTest_test_subscribe_stream_id"
            self.client.create_stream(stream_id,"")
            write_events_count = 1
            events = []
            for i in range(write_events_count):
                event_id = stream_id+"_data_"+str(i)
                events.append(Event(event_id,""))
            self.client.subscribe(stream_id, lambda s: self.assertEquals(s, events[0]))
            self.client.append_to_stream(stream_id, events)
            time.sleep(2)
            self.client.unsubscribe(stream_id)
        except:
            self.assertTrue(False)


    def test_subscribe_many(self):
        try:
            stream_id1 = "SubscribeTest_test_subscribe_stream_id_1"
            self.client.create_stream(stream_id1,"")
            write_events_count = 1
            events = []
            for i in range(write_events_count):
                event_id = stream_id1+"_data_"+str(i)
                events.append(Event(event_id,""))
            self.client.subscribe(stream_id1, lambda s: self.assertEquals(s, events[0]))
            self.client.append_to_stream(stream_id1, events)

            time.sleep(3)

            stream_id2 = "SubscribeTest_test_subscribe_stream_id_2"
            self.client.create_stream(stream_id2,"")
            write_events_count = 1
            events = []
            for i in range(write_events_count):
                event_id = stream_id2+"_data_"+str(i)
                events.append(Event(event_id,""))
            self.client.subscribe(stream_id2, lambda s: self.assertEquals(s, events[0]))
            self.client.append_to_stream(stream_id2, events)

            self.client.unsubscribe(stream_id1)
            self.client.unsubscribe(stream_id2)
            self.assertTrue(True)
        except:
            self.assertTrue(False)