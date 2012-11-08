import unittest;
from EventStoreClient import EventStoreClient;
from Event import Event;
import time;
import os;

class CreateStreamTest(unittest.TestCase):
    __client =  EventStoreClient();

    def test_check_if_stream_will_be_created_after_create_stream(self):
        responce = self.__client.CreateStream("CreateStreamTest_test_check_if_stream_will_be_created_after_create_stream", "");
        self.assertEqual(responce.status, 201);

class DeleteStreamTest(unittest.TestCase):
    __client = EventStoreClient();

    def test_check_if_stream_will_be_deleted_after_delete_stream(self):
        responce = self.__client.CreateStream("DeleteStreamTest_test_check_if_stream_will_be_deleted_after_delete_stream", "");
        self.assertEqual(responce.status, 201);
        responce = self.__client.DeleteStream("DeleteStreamTest_test_check_if_stream_will_be_deleted_after_delete_stream",0);
        self.assertEqual(responce.status, 204);

class AppendToStreamTest(unittest.TestCase):
    __client = EventStoreClient();

    def test_check_if_event_will_append_to_stream_after_append_to_stream_function(self):
        responce = self.__client.CreateStream("AppendToStreamTest_test_check_if_event_will_append_to_stream_after_append_to_stream", "");
        self.assertEqual(responce.status, 201);
        responce = self.__client.AppendToStream("AppendToStreamTest_test_check_if_event_will_append_to_stream_after_append_to_stream", Event("AppendToStreamTest_test_check_if_event_will_append_to_stream_after_append_to_stream_event_data",""),0)
        self.assertEqual(responce.status, 201);

class ReadEventFromStreamTest(unittest.TestCase):
    __client = EventStoreClient();

    def test_check_if_event_data_will_be_same_as_was_wrote(self):
        responce = self.__client.CreateStream("ReadEventFromStreamTest_test_check_if_event_data_will_be_same_as_was_wrote_create_stream", "");
        self.assertEqual(responce.status, 201);
        writeEvent =Event("ReadEventFromStreamTest_test_check_if_event_data_will_be_same_as_was_wrote_event_data","");
        responce = self.__client.AppendToStream("ReadEventFromStreamTest_test_check_if_event_data_will_be_same_as_was_wrote_create_stream", writeEvent,0)
        self.assertEqual(responce.status, 201);
        readEvent = self.__client.ReadEvent("ReadEventFromStreamTest_test_check_if_event_data_will_be_same_as_was_wrote_create_stream",1);
        self.assertEqual(readEvent["data"],writeEvent.data );
        self.assertEqual(readEvent["metadata"],writeEvent.metadata);

class ReadStreamEventsBackwardTest(unittest.TestCase):
    __client = EventStoreClient();

    def test_read_couple_events_forward_from_stream(self):
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
        responce = self.__client.ReadStreamEventsBackward("ReadStreamEventsForwardTest_test_read_couple_events_forward_from_stream_create_stream", startPosition,3);
        for event in responce:
            self.assertEqual(event['data'],"ReadStreamEventsForwardTest_test_read_couple_events_forward_from_stream_event_data"+str(startPosition-1));
            startPosition = startPosition+1;

if __name__ == '__main__':
    os.startfile('D:\\apps\\EventStore\\bin\\eventstore\\debug\\anycpu\\EventStore.SingleNode.exe');
    time.sleep(3);
    unittest.main();