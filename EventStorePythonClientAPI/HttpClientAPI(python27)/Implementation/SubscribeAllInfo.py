class SubscribeAllInfo:
    def __init__(self, commitPosition, preparePosition, callback):
        self.commitPosition = commitPosition
        self.preparePosition = preparePosition
        self.callback = callback