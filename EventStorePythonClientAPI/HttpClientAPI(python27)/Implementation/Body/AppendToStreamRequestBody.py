class AppendToStreamRequestBody:
    def __init__(self, expected_version,events):
        self.expected_version = expected_version
        self.events = events