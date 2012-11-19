class ConnectToServerException(BaseException):
    def __init__(self, exception, msg):
        self.exception = exception
        self.msg = msg
    def __str__(self):
        return msg+exception.value