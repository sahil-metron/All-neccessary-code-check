class CollectorConfigurationException(Exception):

    def __init__(self, code: int, cause: str) -> None:
        self.code: int = code
        self.cause: str = cause

    def __str__(self) -> str:
        return f"{self.__class__.__name__} [CODE:{self.code:04d}] Error in configuration, cause: \"{self.cause}\""
