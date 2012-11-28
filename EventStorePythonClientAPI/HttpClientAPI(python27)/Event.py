import uuid;

class Event:
    def __init__(self, data, metadata, eventId = str(uuid.uuid4())):
        self.data = str(data);
        self.metadata = metadata;
        self.eventId = eventId;
        self.eventType =str(type(data));