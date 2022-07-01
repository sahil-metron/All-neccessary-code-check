class InputCreationException(Exception):

    def __init__(self, code: int, cause: str) -> None:
        self.code: int = code
        self.cause: str = cause

    def __str__(self) -> str:
        return \
            "{exception_name} [CODE:{code:04d}] Error, cause: {cause}".format(
                exception_name=self.__class__.__name__,
                code=self.code,
                cause=self.cause
            )


class ServiceCreationException(Exception):

    def __init__(self, code: int, cause: str) -> None:
        self.code: int = code
        self.cause: str = cause

    def __str__(self) -> str:
        return \
            "{exception_name} [CODE:{code:04d}] Error, cause: {cause}".format(
                exception_name=self.__class__.__name__,
                code=self.code,
                cause=self.cause
            )
