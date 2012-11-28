class CreateStreamRequestBody:
    def __init__(self, eventStreamId, metadata):
        if type(eventStreamId)!=str or type(metadata)!=str:
            raise;
        self.eventStreamId = eventStreamId
        self.metadata = metadata