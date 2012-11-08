import EventStoreClient;
from  Event import Event;

client = EventStoreClient.EventStoreClient();
#result = client.CreateStream("TestStream1", "");
#result = client.CreateStream("TestStream2", "");
#result = client.GetStreams();
#result = client.DeleteStream("TestStream1", 1);
#result = client.AppendToStream("TestStream1",[Event([1,"lalala1"], "metadata"),Event("Lalalalalalalalaaaaaaa", "metadata")], 0);
result = client.ReadEvent("TestStream1", 0)
#result = client.ReadStreamEventsBackward("TestStream1",1, 3);
#result = client.ReadStreamEventsForward("TestStream1",1, 3);
#result = client.ReadAllEventsForward(10,10);
#print(result.status)
#print(result.status, result.reason);
#print(result.msg);
#print(result.read())