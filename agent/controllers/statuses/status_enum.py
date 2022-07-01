from enum import Enum


class StatusEnum(Enum):
    UNKNOWN = 0
    RUNNING = 10
    PAUSED = 20
    STOPPED = 30
    PAUSED_FAILING = 40

    def __str__(self) -> str:
        return f'{self.name}({self.value})'
