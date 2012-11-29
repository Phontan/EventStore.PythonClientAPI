from libs import *

class DeleteStreamAsyncTest(unittest.TestCase):
    __client = ClientAPI();

    def test_delete_stream(self):
        streamId = "DeleteStreamAsyncTest_test_delete_stream_stream_id"
        self.__client.CreateStreamAsync(streamId,"",lambda s: self.OnSipmlySuccess(s),lambda f: self.OnSipmlyFailed(f))
        self.__client.DeleteStreamAsync(streamId, lambda s: self.OnSipmlySuccess(s),lambda f: self.OnSipmlyFailed(f))

    def OnSipmlySuccess(self, resp):
        self.assertEqual(1,1)
    def OnSipmlyFailed(self, resp):
        self.assertEqual(1, 2)


if __name__ == '__main__':
    os.startfile('D:\\apps\\EventStore\\bin\\eventstore\\debug\\anycpu\\EventStore.SingleNode.exe');
    time.sleep(3)
    unittest.main()
