from abc import abstractmethod


class CollectorQueue:

    @abstractmethod
    def get_maxsize(self) -> int:
        pass
