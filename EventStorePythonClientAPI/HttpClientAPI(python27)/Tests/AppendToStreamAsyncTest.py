from libs import *

class AppendToStreamAsyncTest(unittest.TestCase):
    __client = ClientAPI();

    def test_appent_events_to_stream(self):
        streamId = "AppendToStreamAsyncTest_test_appent_events_to_stream_stream_id"
        self.__client.CreateStreamAsync(streamId,"",lambda s: self.OnSipmlySuccess(s),lambda f: self.OnSipmlyFailed(f))
        writeEventsCount = 1234;
        self.FillStream(streamId, writeEventsCount)

    def test_appent_one_event_to_stream(self):
        streamId = "AppendToStreamAsyncTest_test_appent_one_event_to_stream_stream_id"
        self.__client.CreateStreamAsync(streamId,"",lambda s: self.OnSipmlySuccess(s),lambda f: self.OnSipmlyFailed(f))
        eventId = streamId+"_data_eventId"
        self.__client.AppendToStreamAsync(streamId, Event(eventId,""),lambda s: self.OnSipmlySuccess(s),lambda f: self.OnSipmlyFailed(f))

    def OnSipmlySuccess(self, resp):
        self.assertEqual(1,1)
    def OnSipmlyFailed(self, resp):
        self.assertEqual(1,0)
    def FillStream(self, streamId, writeEventsCount):
        events = []
        for i in range(writeEventsCount):
            eventId = streamId+"_data_"+str(i);
            events.append(Event(eventId,""))
        self.__client.AppendToStreamAsync(streamId, events,lambda s: self.OnSipmlySuccess(s),lambda f: self.OnSipmlyFailed(f))



if __name__ == '__main__':
    os.startfile('D:\\apps\\EventStore\\bin\\eventstore\\debug\\anycpu\\EventStore.SingleNode.exe');
    time.sleep(3)
    unittest.main()