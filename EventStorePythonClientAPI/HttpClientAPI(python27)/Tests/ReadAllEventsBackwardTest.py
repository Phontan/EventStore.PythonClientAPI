from libs import *

class ReadAllEventsBackwardTest(unittest.TestCase):
    __client = ClientAPI();

    def test_read_more_than_one_batch_from_last_position(self):
        streamId = "ReadAllEventsBackwardTest_test_read_more_than_one_batch_from_last_position_stream_id"
        try:
            self.__client.CreateStream(streamId,"")
            writeEventsCount = 100
            writeEvents = []
            for i in range(writeEventsCount):
                eventId = streamId+"_data_"+str(i);
                writeEvents.append(Event(eventId,""))
            self.__client.AppendToStream(streamId, writeEvents)
            readEventsCount = 30;
            readEvents = self.__client.ReadAllEventsBackward(-1,-1,readEventsCount)
            for i in range(readEventsCount):
                self.assertEqual(writeEvents[writeEventsCount-i-1].data, readEvents.events[i]['data'])
            self.assertTrue(True)
        except:
            self.assertTrue(False)