from libs import *

class ReadAllEventsForwardTest(unittest.TestCase):
    __client = ClientAPI();

    def test_read_more_than_one_batch_from_last_position(self):
        streamId = "ReadAllEventsForwardTest_test_read_more_than_one_batch_from_last_position_stream_id"
        try:
            self.__client.create_stream(streamId,"")
            sysEvents = self.__client.read_all_events_forward(0,0,10000)
            sysEventsCount = len(sysEvents.events)
            writeEventsCount = 1234
            writeEvents = []
            for i in range(writeEventsCount):
                eventId = streamId+"_data_"+str(i)
                writeEvents.append(Event(eventId,""))
            self.__client.append_to_stream(streamId, writeEvents)
            readEventsCount = 30
            readEvents = self.__client.read_all_events_forward(0,0,readEventsCount)
            readEventsCount = len(readEvents.events)
            for i in range(readEventsCount-sysEventsCount-1):
                self.assertEqual(writeEvents[i].data, readEvents.events[sysEventsCount+i]['data'])
            self.assertTrue(True)
        except:
            self.assertTrue(False)
