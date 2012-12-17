from libs import *
import uuid

class DeleteStreamTest(unittest.TestCase):
    __client = ClientAPI();

    def test_delete_stream(self):
        stream_id = "DeleteStreamTest_test_delete_stream_stream_id"
        try:
            self.__client.create_stream(stream_id)
            self.__client.delete_stream(stream_id)
            self.assertTrue(True)
        except:
            self.assertTrue(False)
