from libs import *

class ReadStreamEventsForwardTest(unittest.TestCase):
    __client = ClientAPI();

    def test_read_more_than_one_batch_from_start(self):
        streamId = "ReadStreamEventsForwardTest_test_read_more_than_one_batch_from_start_stream_id"
        try:
            self.__client.CreateStream(streamId,"")
            writeEventsCount = 1234;
            writeEvents = []
            for i in range(writeEventsCount):
                eventId = streamId+"_data_"+str(i);
                writeEvents.append(Event(eventId,""))
            self.__client.AppendToStream(streamId, writeEvents)
            readEventsCount = 30
            startPosition = 0
            readEvents = self.__client.ReadStreamEventsForward(streamId, startPosition, readEventsCount)
            for i in range(readEventsCount-1):
                self.assertEqual(writeEvents[i].data, readEvents[i+1]['data'])
            self.assertTrue(True)
        except:
            self.assertTrue(False)


    def test_read_less_than_one_batch_from_start(self):
        streamId = "ReadStreamEventsForwardTest_test_read_less_than_one_batch_from_start_stream_id"
        try:
            self.__client.CreateStream(streamId,"")
            writeEventsCount = 1234;
            writeEvents = []
            for i in range(writeEventsCount):
                eventId = streamId+"_data_"+str(i);
                writeEvents.append(Event(eventId,""))
            self.__client.AppendToStream(streamId, writeEvents)
            readEventsCount = 5
            startPosition = 0
            readEvents = self.__client.ReadStreamEventsForward(streamId, startPosition, readEventsCount)
            for i in range(readEventsCount-1):
                self.assertEqual(writeEvents[i].data, readEvents[i+1]['data'])
            self.assertTrue(True)
        except:
            self.assertTrue(False)


    def test_read_events_from_random_position(self):
        streamId = "ReadStreamEventsForwardTest_test_read_events_from_random_position_position_stream_id"
        try:
            self.__client.CreateStream(streamId,"")
            writeEventsCount = 1234;
            writeEvents = []
            for i in range(writeEventsCount):
                eventId = streamId+"_data_"+str(i);
                writeEvents.append(Event(eventId,""))
            self.__client.AppendToStream(streamId, writeEvents)
            readEventsCount = 46;
            offset = 123
            startPosition = writeEventsCount - offset
            readEvents = self.__client.ReadStreamEventsForward(streamId,startPosition, readEventsCount)
            for i in range(readEventsCount):
                self.assertEqual(writeEvents[startPosition+i-1].data, readEvents[i]['data'])
            self.assertTrue(True)
        except:
            self.assertTrue(False)


    def test_read_more_than_one_batch_from_end(self):
        streamId = "ReadStreamEventsForwardTest_test_read_more_than_one_batch_from_end_stream_id"
        try:
            self.__client.CreateStream(streamId,"")
            writeEventsCount = 1234;
            writeEvents = []
            for i in range(writeEventsCount):
                eventId = streamId+"_data_"+str(i);
                writeEvents.append(Event(eventId,""))
            self.__client.AppendToStream(streamId, writeEvents)
            readEventsCount = 100
            startPosition = 1200
            readEvents = self.__client.ReadStreamEventsForward(streamId, startPosition, readEventsCount)
            for i in range(len(readEvents)):
                self.assertEqual(writeEvents[startPosition + i -1].data, readEvents[i]['data'])
            self.assertTrue(True)
        except:
            self.assertTrue(False)