from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional


class CollectorPersistence(ABC):

    def __init__(self, collector_id: str, unique_identifier: str):
        self.__collector_id: str = collector_id
        self.__unique_identifier: str = unique_identifier
        self._last_persisted_timestamp: Optional[datetime] = None
        self.use_old_format = True

    @property
    def collector_id(self) -> str:
        return self.__collector_id

    @property
    def unique_identifier(self) -> str:
        return self.__unique_identifier

    def save_state(self, object_to_save: dict, no_log_traces: bool = False):
        self._save_state(object_to_save, no_log_traces)

    def load_state(self, no_log_traces: bool = False) -> dict:
        return self._load_state(no_log_traces)

    def load_state_old_format(self, no_log_traces: bool = False, unique_identifier: str = None) -> dict:
        return self._load_state_old_format(no_log_traces, unique_identifier)

    def is_ok(self) -> bool:
        return self._is_ok()

    @abstractmethod
    def _save_state(self, object_to_save: dict, no_log_traces: bool):
        pass

    @abstractmethod
    def _load_state(self, no_log_traces: bool) -> dict:
        pass

    @abstractmethod
    def _load_state_old_format(self, no_log_traces: bool, unique_identifier: str) -> dict:
        pass

    @abstractmethod
    def _is_ok(self) -> bool:
        pass
