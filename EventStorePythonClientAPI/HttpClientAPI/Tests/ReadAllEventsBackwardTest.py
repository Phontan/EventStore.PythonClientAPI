from libs import *

class ReadAllEventsBackwardTest(unittest.TestCase):
    __client = EventStoreClient();

    def test_check_if_event_data_will_be_same_as_was_written(self):
        allStreasmAndEvents = {};
        streamsCount = 5;
        streamsWriteIndex = 0;
        while streamsWriteIndex!=streamsCount:
            streamName ="ReadAllEventBackwardTest_test_check_if_event_data_will_be_same_as_was_wrote_create_stream"+str(streamsWriteIndex);
            allStreasmAndEvents[streamName] = list();
            responce = self.__client.CreateStream(streamName, "");
            self.assertEqual(responce.status, 201);

            eventsCount =10;
            eventsWriteIndex = 0;
            while eventsWriteIndex!=eventsCount:
                event  = Event(streamName+"_event_"+str(eventsWriteIndex)+"_data", streamName+"_event_"+str(eventsWriteIndex)+"_metadata");
                allStreasmAndEvents[streamName].append(event);
                responce = self.__client.AppendToStream(streamName, event, eventsWriteIndex);
                eventsWriteIndex+=1;
                self.assertEqual(responce.status, 201)

            streamsWriteIndex+=1;

        readEventsCount = 35;
        responce = self.__client.ReadAllEventsBackward(-1,-1,readEventsCount)["events"];
        streamIndex = len(allStreasmAndEvents)-1;
        eventIndex = 0;
        while eventIndex<readEventsCount:
            streameName ="ReadAllEventBackwardTest_test_check_if_event_data_will_be_same_as_was_wrote_create_stream"+str(streamIndex);
            streamIndex-=1;
            localStreamIndex = len(allStreasmAndEvents[streameName])-1;
            while localStreamIndex>=0 and eventIndex<readEventsCount:
                self.assertEqual(allStreasmAndEvents[streameName][localStreamIndex].data, responce[eventIndex]["data"])
                eventIndex+=1;
                localStreamIndex-=1;
            eventIndex+=1;
