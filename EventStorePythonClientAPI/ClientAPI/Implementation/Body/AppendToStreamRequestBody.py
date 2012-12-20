class AppendToStreamRequestBody:
    def __init__(self, expected_version,events):
        self.expectedVersion = expected_version
        self.events = events