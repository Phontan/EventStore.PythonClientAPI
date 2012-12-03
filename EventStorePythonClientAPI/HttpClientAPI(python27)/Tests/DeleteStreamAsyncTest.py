from libs import *

class DeleteStreamTest(unittest.TestCase):
    __client = ClientAPI();
    __callbackResult = None

    def test_delete_stream(self):
        streamId = "DeleteStreamTest_test_delete_stream_stream_id"
        success = True
        self.__client.CreateStream(streamId,"", lambda s: self.__Success(s), lambda s: self.__Failed(s))
        self.assertTrue(self.__callbackResult)
        self.__client.DeleteStream(streamId, lambda s: self.__Success(s), lambda s: self.__Failed(s))
        self.assertTrue(self.__callbackResult)

    def __Success(self, response):
        self.__callbackResult = True;

    def __Failed(self, response):
        self.__callbackResult = False;

