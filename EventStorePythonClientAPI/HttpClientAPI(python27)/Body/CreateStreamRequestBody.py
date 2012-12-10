import sys
sys.path.append("D:\\apps\\EventStore.PythonClientAPI\\EventStorePythonClientAPI\\HttpClientAPI(python27)");
import Ensure

class CreateStreamRequestBody:
    def __init__(self, eventStreamId, metadata):
        Ensure.IsNotEmptyString(eventStreamId)
        Ensure.IsString(metadata)

        self.eventStreamId = eventStreamId
        self.metadata = metadata