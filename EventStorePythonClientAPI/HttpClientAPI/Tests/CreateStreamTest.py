from libs import *
class CreateStreamTest(unittest.TestCase):
    __client =  EventStoreClient();

    def test_check_if_stream_will_be_created_after_create_stream(self):
        responce = self.__client.CreateStream("CreateStreamTest_test_check_if_stream_will_be_created_after_create_stream", "");
        self.assertEqual(responce.status, 201);

    def test_check_if_stream_will_be_created_after_create_stream_twice(self):
        responce = self.__client.CreateStream("CreateStreamTest_test_check_if_stream_will_be_created_after_create_stream_twice", "");
        self.assertEqual(responce.status, 201);
        responce = self.__client.CreateStream("CreateStreamTest_test_check_if_stream_will_be_created_after_create_stream_twice", "");
        self.assertEqual(responce.status, 201);

    def test_check_if_stream_will_be_created_after_it_was_deleted(self):
        responce = self.__client.CreateStream("CreateStreamTest_test_check_if_stream_will_be_created_after_it_was_deleted", "");
        self.assertEqual(responce.status, 201);
        responce = self.__client.DeleteStream("CreateStreamTest_test_check_if_stream_will_be_created_after_it_was_deleted",0);

        self.assertEquals(responce.reason, "Stream deleted")
        self.assert responce = self.__client.CreateStream("CreateStreamTest_test_check_if_stream_will_be_created_after_it_was_deleted", "");