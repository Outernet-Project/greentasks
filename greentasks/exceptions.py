

class BaseTaskError(Exception):
    """
    All exceptions should inherit from this base exception.
    """
    pass


class InvalidTaskError(BaseTaskError):
    """
    Raised when the passed in task cannot be used because it's instantiation
    fails.
    """
    pass
