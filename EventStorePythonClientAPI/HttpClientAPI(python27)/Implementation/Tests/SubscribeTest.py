from libs import *

class SubscribeAllTest(unittest.TestCase):
    __client = ClientAPI();

    def test_subscribe(self):
        streamId = "SubscribeAllTest_test_subscribe_stream_id"
        try:
            self.__client.CreateStream(streamId,"")
            self.__client.SubscribeAll(lambda s: self.Callback(s))
            time.sleep(3)
            self.assertTrue(True)
        except:
            self.assertTrue(False)

    def Callback(self, response):
        print '1'