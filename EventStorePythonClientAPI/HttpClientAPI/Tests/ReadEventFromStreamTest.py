from libs import *

class ReadEventFromStreamTest(unittest.TestCase):
    __client = EventStoreClient();

    def test_check_if_event_data_will_be_same_as_was_written(self):
        responce = self.__client.CreateStream("ReadEventFromStreamTest_test_check_if_event_data_will_be_same_as_was_wrote_create_stream", "");
        self.assertEqual(responce.status, 201);
        writeEvent =Event("ReadEventFromStreamTest_test_check_if_event_data_will_be_same_as_was_wrote_event_data","");
        responce = self.__client.AppendToStream("ReadEventFromStreamTest_test_check_if_event_data_will_be_same_as_was_wrote_create_stream", writeEvent,0)
        self.assertEqual(responce.status, 201);
        readEvent = self.__client.ReadEvent("ReadEventFromStreamTest_test_check_if_event_data_will_be_same_as_was_wrote_create_stream",1);
        self.assertEqual(readEvent["data"],writeEvent.data );
        self.assertEqual(readEvent["metadata"],writeEvent.metadata);

    def test_try_to_read_from_deleted_stream(self):
        responce = self.__client.CreateStream("ReadEventFromStreamTest_test_try_to_read_from_deleted_stream_create_stream", "");
        self.assertEqual(responce.status, 201);
        writeEvent =Event("ReadEventFromStreamTest_test_try_to_read_from_deleted_stream_event_data","");
        responce = self.__client.AppendToStream("ReadEventFromStreamTest_test_try_to_read_from_deleted_stream_create_stream", writeEvent,0)
        self.assertEqual(responce.status, 201);
        responce = self.__client.DeleteStream("ReadEventFromStreamTest_test_try_to_read_from_deleted_stream_create_stream",1);
        self.assertEqual(responce.status, 204);
        readEvent = self.__client.ReadEvent("ReadEventFromStreamTest_test_try_to_read_from_deleted_stream_create_stream",1);
        self.assertEqual(readEvent,None);

    def test_read_not_existent_event(self):
        responce = self.__client.CreateStream("ReadEventFromStreamTest_test_read_not_existent_event_create_stream", "");
        self.assertEqual(responce.status, 201);
        readEvent = self.__client.ReadEvent("ReadEventFromStreamTest_test_read_not_existent_event_create_stream",1);
        self.assertEqual(readEvent,None);
