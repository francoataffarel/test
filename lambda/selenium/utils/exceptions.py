

class ExceptionHandler(Exception):
    """Exception raised for errors while handling request.
    Attributes:
        code -- http code
        message -- explanation of the error
    """

    def __init__(self, code, message):
        self.code = code
        self.message = message
        super().__init__(self.message)