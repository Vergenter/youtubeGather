class JsonError(Exception):
    """Exception raised for errors in the input.

    Attributes:
        argument -- argument for which the error occurred
        message -- explanation of the error
    """

    def __init__(self, argument, message):
        self.argument = argument
        self.message = message
