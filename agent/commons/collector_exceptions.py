class CollectorException(Exception):

    def __init__(self, code: int, cause: str) -> None:
        self.code: int = code
        self.cause: str = cause

    def __str__(self) -> str:
        return \
            "{exception_name}[CODE:{code:04d}] Error in collector, cause: {cause}".format(
                exception_name=self.__class__.__name__,
                code=self.code,
                cause=self.cause
            )
