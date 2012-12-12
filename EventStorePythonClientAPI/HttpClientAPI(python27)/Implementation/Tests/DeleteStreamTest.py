from libs import *
import uuid

class DeleteStreamTest(unittest.TestCase):
    __client = ClientAPI();

    def test_delete_stream(self):
        streamId = "DeleteStreamTest_test_delete_stream_stream_id"
        try:
            self.__client.CreateStream(streamId)
            self.__client.DeleteStream(streamId)
            self.assertTrue(True)
        except:
            self.assertTrue(False)
