from libs import *

class CreateStreamTest(unittest.TestCase):
    __client = ClientAPI();

    def test_create_stream(self):
        stream_id = "CreateStreamTest_test_create_stream_stream_id"
        try:
            self.__client.create_stream(stream_id,"")
            self.assertTrue(True)
        except:
            self.assertTrue(False)