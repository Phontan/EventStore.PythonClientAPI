from libs import *

class ReadStreamEventsBackwardAsyncTest(unittest.TestCase):
    __client = ClientAPI();

    def test_read_more_than_one_batch(self):
        streamId = "ReadStreamEventsBackwardAsyncTest_test_read_more_than_one_batch_stream_id"
        self.__client.CreateStreamAsync(streamId,"",lambda s: self.OnSipmlySuccess(s),lambda f: self.OnSipmlyFailed(f))
        writeEventsCount = 1234;
        self.FillStream(streamId, writeEventsCount)

        readEventsCount = 100;
        i = 0
        expectedEvents =[]
        while i<readEventsCount:
            eventId = streamId+"_data_"+str(i);
            expectedEvents.append(Event(eventId,""))
            i+=1



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