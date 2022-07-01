class BaseModelError(Exception):

    def __init__(self, code: int, cause: str) -> None:
        self.code: int = code
        self.cause: str = cause

    def __str__(self) -> str:
        return \
            "{exception_name} [CODE:{code:04d}] reason: {cause}".format(
                exception_name=self.__class__.__name__,
                code=self.code,
                cause=self.cause
            )


class InitVariablesError(BaseModelError):
    def __init__(self, code: int, cause: str) -> None:
        super().__init__(code, cause)


class SetupError(BaseModelError):
    def __init__(self, code: int, cause: str) -> None:
        super().__init__(code, cause)


class PrePullError(BaseModelError):
    def __init__(self, code: int, cause: str) -> None:
        super().__init__(code, cause)


class PullError(BaseModelError):
    def __init__(self, code: int, cause: str) -> None:
        super().__init__(code, cause)


class ApiError(BaseModelError):
    def __init__(self, code: int, cause: str) -> None:
        super().__init__(code, cause)
