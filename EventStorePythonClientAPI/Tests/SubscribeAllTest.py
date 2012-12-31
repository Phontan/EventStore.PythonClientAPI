from libs import *

class SubscribeAllTest(unittest.TestCase):
    client = ClientAPI()

    def test_subscribe_all(self):
        streamId = "SubscribeAllTest_test_subscribe_all_stream_id"
        try:
            self.client.create_stream(streamId,"")
            write_events_count = 1
            events = []
            for i in range(write_events_count):
                event_id = streamId+"_data_"+str(i)
                events.append(WriteEvent(event_id,""))
            self.client.subscribe_all(lambda s: self.assertEquals(s, events[0]))

            self.client.append_to_stream(streamId, events)
            time.sleep(2)
            self.assertTrue(True)
        except:
            self.assertTrue(False)