class CreateStreamRequestBody:
    def __init__(self, stream_id, metadata):
        self.eventStreamId = stream_id
        self.metadata = metadata