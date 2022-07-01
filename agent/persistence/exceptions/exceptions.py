class SavePersistenceException(Exception):

    def __init__(self, code: int, cause: str) -> None:
        self.code: int = code
        self.cause: str = cause

    def __str__(self) -> str:
        return \
            "{exception_name} [CODE:{code:04d}] Error when saving data, cause: {cause}".format(
                exception_name=self.__class__.__name__,
                code=self.code,
                cause=self.cause
            )


class LoadPersistenceException(Exception):

    def __init__(self, code: int, cause: str) -> None:
        self.code: int = code
        self.cause: str = cause

    def __str__(self) -> str:
        return \
            "{exception_name} [CODE:{code:04d}] Error when loading data, cause: {cause}".format(
                exception_name=self.__class__.__name__,
                code=self.code,
                cause=self.cause
            )


class ObjectPersistenceException(Exception):

    def __init__(self, code: int, cause: str) -> None:
        self.code: int = code
        self.cause: str = cause

    def __str__(self) -> str:
        return \
            "{exception_name} [CODE:{code:04d}] Error with persistence object, cause: {cause}".format(
                exception_name=self.__class__.__name__,
                code=self.code,
                cause=self.cause
            )


class PersistenceManagerException(Exception):

    def __init__(self, code: int, cause: str) -> None:
        self.code: int = code
        self.cause: str = cause

    def __str__(self) -> str:
        return \
            "{exception_name} [CODE:{code:04d}] Error with persistence manager, cause: {cause}".format(
                exception_name=self.__class__.__name__,
                code=self.code,
                cause=self.cause
            )
