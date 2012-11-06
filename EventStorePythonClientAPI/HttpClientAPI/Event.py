import uuid;
import json;

class Event:
    def __init__(self, data, metadata, eventId = str(uuid.uuid4())):
        self.data = str(data);
        self.metadata = metadata;
        self.eventId = eventId;
        self.eventType =str(type(data));

def convert_to_builtin_type(obj):
     d ={}
     d.update(obj.__dict__)
     return d