class CreateStreamRequestBody:
    def __init__(self, eventStreamId, metadata):
        self.eventStreamId = eventStreamId
        self.metadata = metadata