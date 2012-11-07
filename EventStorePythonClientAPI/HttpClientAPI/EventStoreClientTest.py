import unittest;
from EventStoreClient import EventStoreClient;

class CreateStream(unittest.TestCase):
    def setUp(self):
        self.__client =  EventStoreClient();

    def check_if_stream_will_be_created_after_create_stream(self):
        responce = self.__client.CreateStream("TestStreamForUnitTesting");
        self.assertEqual(responce.status, 201);


if __name__ == '__main__':
    unittest.main()