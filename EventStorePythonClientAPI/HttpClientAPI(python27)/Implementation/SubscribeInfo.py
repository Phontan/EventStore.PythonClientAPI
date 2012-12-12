class SubscribeInfo:
    def __init__(self, streamId, lastPosition, callback):
        self.streamId = streamId
        self.lastPosition = lastPosition
        self.callback = callback