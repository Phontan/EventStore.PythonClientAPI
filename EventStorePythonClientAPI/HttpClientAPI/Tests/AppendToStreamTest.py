from libs import *

class AppendToStreamTest(unittest.TestCase):
    __client = EventStoreClient();

    def test_check_if_event_will_append_to_stream_after_append_to_stream_function(self):
        responce = self.__client.CreateStream("AppendToStreamTest_test_check_if_event_will_append_to_stream_after_append_to_stream", "");
        self.assertEqual(responce.status, 201);
        responce = self.__client.AppendToStream("AppendToStreamTest_test_check_if_event_will_append_to_stream_after_append_to_stream", Event("AppendToStreamTest_test_check_if_event_will_append_to_stream_after_append_to_stream_event_data",""),0)
        self.assertEqual(responce.status, 201);

    def test_check_if_event_will_append_to_stream_after_append_to_stream_with_same_eventId(self):
        responce = self.__client.CreateStream("test_check_if_event_will_append_to_stream_after_append_to_stream_with_same_eventId_create_stream", "");
        self.assertEqual(responce.status, 201);
        eventId =str(uuid.uuid4());
        responce = self.__client.AppendToStream("test_check_if_event_will_append_to_stream_after_append_to_stream_with_same_eventId_create_stream", Event("test_check_if_event_will_append_to_stream_after_append_to_stream_with_same_eventId_event_data_first","",eventId),0)
        self.assertEqual(responce.status, 201);
        responce = self.__client.AppendToStream("test_check_if_event_will_append_to_stream_after_append_to_stream_with_same_eventId_create_stream", Event("test_check_if_event_will_append_to_stream_after_append_to_stream_with_same_eventId_event_data_second","",eventId),0)
        self.assertEqual(responce.status, 201);
