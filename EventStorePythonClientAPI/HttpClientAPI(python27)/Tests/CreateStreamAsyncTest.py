from libs import *

class CreateStreamTest(unittest.TestCase):
    __client = ClientAPI();

    def test_create_stream(self):
        streamId = "CreateStreamTest_test_create_stream_stream_id"
        try:
            self.__client.CreateStream(streamId,"")
            self.assertTrue(True)
        except:
            self.assertTrue(False)