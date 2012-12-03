from libs import *

class CreateStreamTest(unittest.TestCase):
    __client = ClientAPI();
    __callbackResult = None;

    def test_create_stream(self):
        streamId = "CreateStreamTest_test_create_stream_stream_id"
        self.__client.CreateStream(streamId,"",lambda s: self.__Success(s),lambda f: self.__Failed(f))
        self.assertTrue(self.__callbackResult)

    def __Success(self, response):
        self.__callbackResult = True;

    def __Failed(self, response):
        self.__callbackResult = False;