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

##
##    def test_read_less_than_one_batch_from_last_position(self):
##        streamId = "ReadStreamEventsBackwardTest_test_read_less_than_one_batch_from_last_position_stream_id"
##        try:
##            self.__client.CreateStream(streamId,"")
##            writeEventsCount = 1234;
##            writeEvents = []
##            for i in range(writeEventsCount):
##                eventId = streamId+"_data_"+str(i);
##                writeEvents.append(Event(eventId,""))
##            self.__client.AppendToStream(streamId, writeEvents)
##            readEventsCount = 5;
##            readEvents = self.__client.ReadStreamEventsBackward(streamId,writeEventsCount, readEventsCount)
##            for i in range(readEventsCount):
##                self.assertEqual(writeEvents[writeEventsCount-i-1].data, readEvents[i]['data'])
##            self.assertTrue(True)
##        except:
##            self.assertTrue(False)
##
##
##    def test_read_less_than_one_batch_from_random_position(self):
##        streamId = "ReadStreamEventsBackwardTest_test_read_less_than_one_batch_from_random_position_stream_id"
##        try:
##            self.__client.CreateStream(streamId,"")
##            writeEventsCount = 1234;
##            writeEvents = []
##            for i in range(writeEventsCount):
##                eventId = streamId+"_data_"+str(i);
##                writeEvents.append(Event(eventId,""))
##            self.__client.AppendToStream(streamId, writeEvents)
##            readEventsCount = 5;
##            offset = 123
##            startPosition = writeEventsCount - offset
##            readEvents = self.__client.ReadStreamEventsBackward(streamId,startPosition, readEventsCount)
##            for i in range(readEventsCount):
##                self.assertEqual(writeEvents[startPosition-i-1].data, readEvents[i]['data'])
##            self.assertTrue(True)
##        except:
##            self.assertTrue(False)
##
##
##    def test_read_more_than_one_batch_from_random_position(self):
##        streamId = "ReadStreamEventsBackwardTest_test_read_more_than_one_batch_from_random_position_stream_id"
##        try:
##            self.__client.CreateStream(streamId,"")
##            writeEventsCount = 1234;
##            writeEvents = []
##            for i in range(writeEventsCount):
##                eventId = streamId+"_data_"+str(i);
##                writeEvents.append(Event(eventId,""))
##            self.__client.AppendToStream(streamId, writeEvents)
##            readEventsCount = 50;
##            offset = 123
##            startPosition = writeEventsCount - offset
##            readEvents = self.__client.ReadStreamEventsBackward(streamId,startPosition, readEventsCount)
##            for i in range(readEventsCount):
##                self.assertEqual(writeEvents[startPosition-i-1].data, readEvents[i]['data'])
##            self.assertTrue(True)
##        except:
##            self.assertTrue(False)
##
##
##    def test_read_couple_events_from_not_exist_position(self):
##        streamId = "ReadStreamEventsBackwardTest_test_read_couple_events_from_not_exist_position_stream_id"
##        try:
##            self.__client.CreateStream(streamId,"")
##            writeEventsCount = 1234;
##            writeEvents = []
##            for i in range(writeEventsCount):
##                eventId = streamId+"_data_"+str(i);
##                writeEvents.append(Event(eventId,""))
##            self.__client.AppendToStream(streamId, writeEvents)
##            readEventsCount = 50;
##            offset = -123
##            startPosition = writeEventsCount - offset
##            readEvents = self.__client.ReadStreamEventsBackward(streamId,startPosition, readEventsCount)
##            self.assertEqual(readEvents, [])
##        except:
##            self.assertTrue(False)
##
##
##    def test_read_many_events_from_not_exist_position(self):
##        streamId = "ReadStreamEventsBackwardTest_test_read_many_events_from_not_exist_position_stream_id"
##        try:
##            self.__client.CreateStream(streamId,"")
##            writeEventsCount = 1234;
##            writeEvents = []
##            for i in range(writeEventsCount):
##                eventId = streamId+"_data_"+str(i);
##                writeEvents.append(Event(eventId,""))
##            self.__client.AppendToStream(streamId, writeEvents)
##            readEventsCount = 100
##            offset = -50
##            startPosition = writeEventsCount - offset
##            readEvents = self.__client.ReadStreamEventsBackward(streamId,startPosition, readEventsCount)
##            for i in range(readEventsCount+offset):
##                self.assertEqual(writeEvents[writeEventsCount-i-1].data, readEvents[i]['data'])
##        except:
##            self.assertTrue(False)
##
##
##    def test_read_many_events_from_start(self):
##        streamId = "ReadStreamEventsBackwardTest_test_read_many_events_from_start_stream_id"
##        try:
##            self.__client.CreateStream(streamId,"")
##            writeEventsCount = 1234;
##            writeEvents = []
##            for i in range(writeEventsCount):
##                eventId = streamId+"_data_"+str(i);
##                writeEvents.append(Event(eventId,""))
##            self.__client.AppendToStream(streamId, writeEvents)
##            readEventsCount = 100
##            startPosition = 10
##            readEvents = self.__client.ReadStreamEventsBackward(streamId,startPosition, readEventsCount)
##            for i in range(startPosition-1):
##                self.assertEqual(readEvents[startPosition-i-1]['data'], writeEvents[i].data)
##        except:
##            self.assertTrue(False)