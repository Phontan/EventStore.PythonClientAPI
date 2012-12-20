import uuid

class Event:
    def __init__(self, data, metadata='', eventId = None, eventType=None):
        self.data = str(data)
        self.metadata = metadata
        if eventId is None:
            self.eventId = str(uuid.uuid4())
        else:
            self.eventId = eventId
        if eventType is None:
            self.eventType =str(type(data))
        else:
            self.eventType = eventType