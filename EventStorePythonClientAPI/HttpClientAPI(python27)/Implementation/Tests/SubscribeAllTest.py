from libs import *

class SubscribeAllTest(unittest.TestCase):
    __client = ClientAPI();

    def test_subscribe(self):
        streamId = "CreateStreamTest_test_subscribe_stream_id"
        try:
            self.__client.CreateStream(streamId,"")
            self.__client.SubscribeAll(lambda s: self.Callback(s))
            writeEventsCount = 1234;
            events = []
            for i in range(writeEventsCount):
                eventId = streamId+"_data_"+str(i);
                events.append(Event(eventId,""))
            self.__client.AppendToStream(streamId, events)
            self.assertTrue(True)
        except:
            self.assertTrue(False)

    def Callback(self, response):
        print response[0]