class SubscribeAllInfo:
    def __init__(self, commit_position, prepare_position, callback):
        self.commit_position = commit_position
        self.prepare_position = prepare_position
        self.callback = callback