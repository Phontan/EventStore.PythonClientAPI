import EventStoreClient;
from  Event import Event;

client = EventStoreClient.EventStoreClient();
#result = client.CreateStream("TestStream1", "");
#result = client.GetStreams();
#result = client.DeleteStream("TestStream1", 0);
#result = client.AppendToStream("TestStream1",[Event([1,"lalala1"], "metadata"),Event("Lalalalalalalalaaaaaaa", "metadata")], 3);
result = client.ReadEvent("TestStream1", 5)
print(result.status, result.reason);
print(result.msg);
print(result.read())