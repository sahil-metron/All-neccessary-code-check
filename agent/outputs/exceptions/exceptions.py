class DevoSenderException(Exception):

    def __init__(self, code: int, cause: str) -> None:
        self.code: int = code
        self.cause: str = cause

    def __str__(self) -> str:
        return \
            "{exception_name}[CODE:{code:04d}] Error when sending data to Devo platform, cause: {cause}".format(
                exception_name=self.__class__.__name__,
                code=self.code,
                cause=self.cause
            )


class SenderManagerAbstractException(Exception):

    def __init__(self, code: int, cause: str) -> None:
        self.code: int = code
        self.cause: str = cause

    def __str__(self) -> str:
        return \
            "{exception_name}[CODE:{code:04d}] Error found in SenderManagerAbstract class, cause: {cause}".format(
                exception_name=self.__class__.__name__,
                code=self.code,
                cause=self.cause
            )


class ConsoleSenderException(Exception):

    def __init__(self, code: int, cause: str) -> None:
        self.code: int = code
        self.cause: str = cause

    def __str__(self) -> str:
        return \
            "{exception_name}[CODE:{code:04d}] Error when sending data to Console, cause: {cause}".format(
                exception_name=self.__class__.__name__,
                code=self.code,
                cause=self.cause
            )


class SyslogSenderException(Exception):

    def __init__(self, code: int, cause: str) -> None:
        self.code: int = code
        self.cause: str = cause

    def __str__(self) -> str:
        return \
            "{exception_name}[CODE:{code:04d}] Error when sending data to Syslog server, cause: {cause}".format(
                exception_name=self.__class__.__name__,
                code=self.code,
                cause=self.cause
            )
