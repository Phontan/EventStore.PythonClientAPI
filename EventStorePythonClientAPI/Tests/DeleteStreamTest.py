from libs import *
import uuid

class DeleteStreamTest(unittest.TestCase):
    __client = ClientAPI()

    def test_delete_stream(self):
        stream_id = "DeleteStreamTest_test_delete_stream_stream_id"
        try:
            self.__client.create_stream(stream_id)
            self.__client.delete_stream(stream_id)
            self.assertTrue(True)
        except:
            self.assertTrue(False)

    def test_delete_stream_with_events(self):
        stream_id = "DeleteStreamTest_test_delete_stream_with_events_stream_id"
        try:
            self.__client.create_stream(stream_id)
            for i in range(1):
                self.__client.append_to_stream(stream_id, WriteEvent("{0}_data_{1}".format(stream_id, i)))
            self.__client.delete_stream(stream_id)
            self.assertTrue(True)
        except:
            self.assertTrue(False)
