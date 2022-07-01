import logging

from agent.persistence.base_manager import CollectorPersistenceManager
from agent.persistence.memory.memory import MemoryPersistence

log = logging.getLogger(__name__)


class MemoryPersistenceManager(CollectorPersistenceManager):

    def __init__(self, collector_id: str, persistence_config: dict):
        super().__init__(collector_id, "memory")

    def get_instance_for_unique_identifier(self, unique_identifier: str) -> MemoryPersistence:
        return MemoryPersistence(self.collector_id, unique_identifier)

    def __str__(self):
        return \
            f'{{' \
            f'"class": "{type(self).__name__}", ' \
            f'"type": "{self.persistence_type}", ' \
            f'"config": null' \
            f'}}'
