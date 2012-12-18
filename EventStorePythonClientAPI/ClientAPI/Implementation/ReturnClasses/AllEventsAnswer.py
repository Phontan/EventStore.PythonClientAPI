class AllEventsAnswer:
    def __init__(self, events, prepare_position, commit_position):
        self.events = events;
        self.prepare_position = prepare_position;
        self.commit_position = commit_position;