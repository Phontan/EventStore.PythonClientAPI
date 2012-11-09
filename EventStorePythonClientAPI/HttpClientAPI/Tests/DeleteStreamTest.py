from libs import *

class DeleteStreamTest(unittest.TestCase):
    __client = EventStoreClient();

    def test_check_if_stream_will_be_deleted_after_delete_stream(self):
        responce = self.__client.CreateStream("DeleteStream_Test_test_check_if_stream_will_be_deleted_after_delete_stream", "");
        self.assertEqual(responce.status, 201);
        responce = self.__client.DeleteStream("DeleteStream_Test_test_check_if_stream_will_be_deleted_after_delete_stream",0);
        self.assertEqual(responce.status, 204);

    def test_check_if_stream_will_be_deleted_after_it_is_already_deleted(self):
        responce = self.__client.CreateStream("DeleteStream_test_check_if_stream_will_be_deleted_after_it_is_already_deleted", "")
        self.assertEqual(responce.status, 201);
        responce = self.__client.DeleteStream("DeleteStream_test_check_if_stream_will_be_deleted_after_it_is_already_deleted",0);
        self.assertEqual(responce.status, 204);
        responce = self.__client.DeleteStream("DeleteStream_test_check_if_stream_will_be_deleted_after_it_is_already_deleted",0);
        self.assertEqual(responce.status, 400);

    def test_delete_not_created_stream(self):
        responce = self.__client.DeleteStream("DeleteStream_test_delete_not_created_stream",0);
        self.assertEqual(responce.status, 400);
