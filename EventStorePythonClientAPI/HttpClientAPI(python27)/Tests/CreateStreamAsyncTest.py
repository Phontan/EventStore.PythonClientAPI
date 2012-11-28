from libs import *

class CreateStreamAsyncTest(unittest.TestCase):
    __client = ClientAPI();

    def test_chack_if_stream_will_create(self):
        streamId = "CreateStreamAsyncTest_test_chack_if_stream_will_create"
        #self.__client.CreateStreamAsync()