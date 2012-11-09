from libs import *

class ReadAllEventsForwardTest(unittest.TestCase):
    __client = EventStoreClient();

    def test_check_if_event_data_will_be_same_as_was_written(self):
        allStreasmAndEvents = {};
        prepareRead = self.__client.ReadAllEvents();
        streamsCount = 5;
        streamsWriteIndex = 0;
        while streamsWriteIndex!=streamsCount:
            streamName ="ReadAllEventForwardTest_test_check_if_event_data_will_be_same_as_was_wrote_create_stream"+str(streamsWriteIndex);
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
        responce = self.__client.ReadAllEventsForward(prepareRead["preparePosition"],prepareRead["commitPosition"],readEventsCount);
        responceData = responce["events"];
        readStreamsIndex = 0;
        readEventsIndex = 0;
        while readEventsIndex<readEventsCount:
            streamName ="ReadAllEventForwardTest_test_check_if_event_data_will_be_same_as_was_wrote_create_stream"+str(readStreamsIndex);
            readStreamsIndex+=1;
            eventsInCurrentStream =  allStreasmAndEvents[streamName];
            eventsInCurrentStreamCount =len(eventsInCurrentStream);
            eventIndex = 0;
            readEventsIndex+=1;
            while eventIndex<eventsInCurrentStreamCount and readEventsIndex<readEventsCount:
                self.assertEqual(eventsInCurrentStream[eventIndex].data, responceData[readEventsIndex]['data'])
                eventIndex+=1;
                readEventsIndex +=1;