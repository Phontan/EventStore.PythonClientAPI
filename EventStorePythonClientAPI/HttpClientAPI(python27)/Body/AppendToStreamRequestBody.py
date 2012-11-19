class AppendToStreamRequestBody:
    def __init__(self, expectedVersion,events):
        self.expectedVersion = expectedVersion
        self.events = events