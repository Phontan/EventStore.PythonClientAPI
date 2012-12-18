class SubscribeInfo:
    def __init__(self, stream_id, last_position, callback):
        self.stream_id = stream_id
        self.last_position = last_position
        self.callback = callback