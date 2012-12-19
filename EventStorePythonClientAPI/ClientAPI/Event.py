import uuid

class Event:
    def __init__(self, data, metadata='', eventId = str(uuid.uuid4()), eventType=None):
        self.data = str(data)
        self.metadata = metadata
        self.eventId = eventId
        if eventType is None:
            self.eventType =str(type(data))
        else:
            self.eventType = eventType