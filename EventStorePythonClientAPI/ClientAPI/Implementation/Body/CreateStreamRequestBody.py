import sys, os
sys.path.append(os.path.dirname(__file__)+"\\..")
import Ensure

class CreateStreamRequestBody:
    def __init__(self, event_stream_id, metadata):
        Ensure.is_not_empty_string(event_stream_id, "event_stream_id")
        Ensure.is_string(metadata, "metadata")

        self.eventStreamId = event_stream_id
        self.metadata = metadata