import sys, os
sys.path.append(os.path.dirname(__file__)+"\\..")
import Ensure

class CreateStreamRequestBody:
    def __init__(self, eventStreamId, metadata):
        Ensure.IsNotEmptyString(eventStreamId, "eventStreamId")
        Ensure.IsString(metadata, "metadata")

        self.eventStreamId = eventStreamId
        self.metadata = metadata