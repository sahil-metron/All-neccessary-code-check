from datetime import datetime
from typing import Optional

from agent.persistence.base import CollectorPersistence


class MemoryPersistence(CollectorPersistence):

    def __init__(self, collector_id: str, unique_identifier: str) -> None:
        super().__init__(collector_id, unique_identifier)
        self.__memory_object: Optional[dict] = None

    def _save_state(self, object_to_save: dict, no_log_traces: bool) -> None:
        self._last_persisted_timestamp = datetime.now()
        self.__memory_object = object_to_save

    def _load_state(self, no_log_traces: bool) -> dict:
        return self.__memory_object

    def _load_state_old_format(self, no_log_traces: bool, unique_identifier) -> dict:
        return self.__memory_object

    def _is_ok(self) -> bool:
        return True

    def __str__(self):
        return \
            f'{{' \
            f'"unique_identifier": {self.unique_identifier}, ' \
            f'"class": "{type(self).__name__}", ' \
            f'"last_persisted_timestamp": {self._last_persisted_timestamp}, ' \
            f'"memory_object": {self.__memory_object}' \
            f'}}'
