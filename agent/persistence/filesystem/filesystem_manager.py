import logging

from agent.persistence.base_manager import CollectorPersistenceManager
from agent.persistence.exceptions.exceptions import PersistenceManagerException
from agent.persistence.filesystem.filesystem import FilesystemPersistence

log = logging.getLogger(__name__)


class FilesystemPersistenceManager(CollectorPersistenceManager):

    def __init__(self, collector_id: str, persistence_config: dict):
        super().__init__(collector_id, "filesystem")
        self.base_directory_name: str = persistence_config.get("directory_name")
        if self.base_directory_name is None:
            raise PersistenceManagerException(
                0, f"{type(self).__name__} -> Configuration is empty"
            )

    def get_instance_for_unique_identifier(self, unique_identifier: str) -> FilesystemPersistence:
        return FilesystemPersistence(self.base_directory_name, self.collector_id, unique_identifier)

    def __str__(self):
        return \
            f'{{' \
            f'"class": "{type(self).__name__}", ' \
            f'"type": "{self.persistence_type}", ' \
            f'"config": {{' \
            f'"directory_name": "{self.base_directory_name}"' \
            f'}}' \
            f'}}'
