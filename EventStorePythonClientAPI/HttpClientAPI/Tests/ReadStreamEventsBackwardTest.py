from libs import *;

class ReadStreamEventsBackwardTest(unittest.TestCase):
    __client = EventStoreClient();

    def test_read_couple_events_backward_from_streama(self):
        responce = self.__client.CreateStream("ReadStreamEventsBackwardTest_test_read_couple_events_backward_from_stream_create_stream", "");
        self.assertEqual(responce.status, 201);
        index = 0;
        allEventsCount = 10
        while(index!=allEventsCount):
            writeEvent =Event("ReadStreamEventsBackwardTest_test_read_couple_events_backward_from_stream_event_data"+str(index),"");
            responce = self.__client.AppendToStream("ReadStreamEventsBackwardTest_test_read_couple_events_backward_from_stream_create_stream", writeEvent,index)
            index=index+1;
            self.assertEqual(responce.status, 201);

        startPosition = 6;
        responce = self.__client.ReadStreamEventsBackward("ReadStreamEventsBackwardTest_test_read_couple_events_backward_from_stream_create_stream", startPosition,3);
        for event in responce:
            self.assertEqual(event['data'],"ReadStreamEventsBackwardTest_test_read_couple_events_backward_from_stream_event_data"+str(startPosition-1));
            startPosition = startPosition-1;

    def test_read_couple_events_backward_from_stream_starts_from_not_existen_position(self):
        responce = self.__client.CreateStream("ReadStreamEventsBackwardTest_test_read_couple_events_backward_from_stream_starts_from_not_existen_position_create_stream", "");
        self.assertEqual(responce.status, 201);
        index = 0;
        allEventsCount = 10
        while(index!=allEventsCount):
            writeEvent =Event("ReadStreamEventsBackwardTest_test_read_couple_events_backward_from_stream_starts_from_not_existen_position_event_data"+str(index),"");
            responce = self.__client.AppendToStream("ReadStreamEventsBackwardTest_test_read_couple_events_backward_from_stream_starts_from_not_existen_position_create_stream", writeEvent,index)
            index=index+1;
            self.assertEqual(responce.status, 201);

        startPosition = 12;
        responce = self.__client.ReadStreamEventsBackward("ReadStreamEventsBackwardTest_test_read_couple_events_backward_from_stream_starts_from_not_existen_position_create_stream", startPosition,3);
        startPosition = allEventsCount;
        for event in responce:
            self.assertEqual(event['data'],"ReadStreamEventsBackwardTest_test_read_couple_events_backward_from_stream_starts_from_not_existen_position_event_data"+str(startPosition-1));
            startPosition = startPosition-1;

    def test_read_not_existen_events_backward_from_stream_starts_from_not_existen_position(self):
        responce = self.__client.CreateStream("ReadStreamEventsBackwardTest_test_read_couple_events_backward_from_stream_starts_from_not_existen_position_create_stream", "");
        self.assertEqual(responce.status, 201);
        index = 0;
        allEventsCount = 10
        while(index!=allEventsCount):
            writeEvent =Event("ReadStreamEventsBackwardTest_test_read_couple_events_backward_from_stream_starts_from_not_existen_position_event_data"+str(index),"");
            responce = self.__client.AppendToStream("ReadStreamEventsBackwardTest_test_read_couple_events_backward_from_stream_starts_from_not_existen_position_create_stream", writeEvent,index)
            index=index+1;
            self.assertEqual(responce.status, 201);

        startPosition = 15;
        responce = self.__client.ReadStreamEventsBackward("ReadStreamEventsBackwardTest_test_read_couple_events_backward_from_stream_starts_from_not_existen_position_create_stream", startPosition,3);
        self.assertEqual(responce, []);
