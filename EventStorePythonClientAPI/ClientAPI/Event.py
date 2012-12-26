import uuid
from Implementation import Ensure

class Event:
    def __init__(self, data, metadata="", event_id = None, event_type=None, is_json = False):
        Ensure.is_bool(is_json, "is_json")
        self.isJson = is_json
        self.data = data if is_json else str(data)
        self.metadata = metadata
        if event_id is None:
            self.eventId = str(uuid.uuid4())
        else:
            self.eventId = event_id
        if event_type is None:
            self.eventType = str(type(data))
        else:
            self.eventType = event_type