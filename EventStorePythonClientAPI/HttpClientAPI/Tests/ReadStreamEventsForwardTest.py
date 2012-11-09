from libs import *

class ReadStreamEventsForwardTest(unittest.TestCase):
    __client = EventStoreClient();

    def test_read_couple_events_forward_from_stream(self):
        responce = self.__client.CreateStream("ReadStreamEventsForwardTest_test_read_couple_events_forward_from_stream_create_stream", "");
        self.assertEqual(responce.status, 201);
        index = 0;
        allEventsCount = 10
        while(index!=allEventsCount):
            writeEvent =Event("ReadStreamEventsForwardTest_test_read_couple_events_forward_from_stream_event_data"+str(index),"");
            responce = self.__client.AppendToStream("ReadStreamEventsForwardTest_test_read_couple_events_forward_from_stream_create_stream", writeEvent,index)
            index=index+1;
            self.assertEqual(responce.status, 201);

        startPosition = 6;
        responce = self.__client.ReadStreamEventsForward("ReadStreamEventsForwardTest_test_read_couple_events_forward_from_stream_create_stream", startPosition,3);
        for event in responce:
            self.assertEqual(event['data'],"ReadStreamEventsForwardTest_test_read_couple_events_forward_from_stream_event_data"+str(startPosition-1));
            startPosition = startPosition+1;

    def test_read_couple_events_forward_from_stream_starts_from_not_existen_position(self):
        responce = self.__client.CreateStream("ReadStreamEventsForwardTest_test_read_couple_events_forward_from_stream_starts_from_not_existen_position_create_stream", "");
        self.assertEqual(responce.status, 201);
        index = 0;
        allEventsCount = 10
        while(index!=allEventsCount):
            writeEvent =Event("ReadStreamEventsForwardTest_test_read_couple_events_forward_from_stream_starts_from_not_existen_position_event_data"+str(index),"");
            responce = self.__client.AppendToStream("ReadStreamEventsForwardTest_test_read_couple_events_forward_from_stream_starts_from_not_existen_position_create_stream", writeEvent,index)
            index=index+1;
            self.assertEqual(responce.status, 201);

        startPosition = 12;
        responce = self.__client.ReadStreamEventsForward("ReadStreamEventsForwardTest_test_read_couple_events_forward_from_stream_starts_from_not_existen_position_create_stream", startPosition,3);
        self.assertEqual(responce, [])

    def test_read_existen_events_forward_from_stream_starts_from_existen_position_and_ends_father_than_streams_end(self):
        responce = self.__client.CreateStream("ReadStreamEventsForwardTest_test_read_existen_events_forward_from_stream_starts_from_existen_position_and_ends_father_than_streams_end_create_stream", "");
        self.assertEqual(responce.status, 201);
        index = 0;
        allEventsCount = 10
        while(index!=allEventsCount):
            writeEvent =Event("ReadStreamEventsForwardTest_test_read_existen_events_forward_from_stream_starts_from_existen_position_and_ends_father_than_streams_end_event_data"+str(index),"");
            responce = self.__client.AppendToStream("ReadStreamEventsForwardTest_test_read_existen_events_forward_from_stream_starts_from_existen_position_and_ends_father_than_streams_end_create_stream", writeEvent,index)
            index=index+1;
            self.assertEqual(responce.status, 201);

        startPosition = 8;
        responce = self.__client.ReadStreamEventsBackward("ReadStreamEventsForwardTest_test_read_existen_events_forward_from_stream_starts_from_existen_position_and_ends_father_than_streams_end_create_stream", startPosition,6);
        for event in responce:
            self.assertEqual(event['data'],"ReadStreamEventsForwardTest_test_read_existen_events_forward_from_stream_starts_from_existen_position_and_ends_father_than_streams_end_event_data"+str(startPosition-1));
            startPosition = startPosition-1;