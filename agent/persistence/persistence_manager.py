import logging

from agent.persistence import persistence_types

log = logging.getLogger(__name__)


class PersistenceManager:

    persistence_instances: dict = {}

    def __init__(self, collector_id: str, persistence_config):
        self.persistence_manager_instance = None
        persistence_class = None
        persistence_type = None
        if isinstance(persistence_config, dict):
            persistence_type = persistence_config.get("type")
            persistence_config = persistence_config.get("config")
            if persistence_type:
                persistence_class = persistence_types[persistence_type]

        if persistence_type is None:
            persistence_class = persistence_types["memory"]
        if persistence_class:
            self.persistence_manager_instance = persistence_class(collector_id, persistence_config)

    def get_instance_for_module(self, collector_instance):
        """

        :param collector_instance:
        :return:
        """

        if self.persistence_manager_instance:
            collector_unique_identifier = "{}_{}_{}_{}_{}".format(
                    collector_instance.input_name,
                    collector_instance.input_id,
                    collector_instance.service_name,
                    collector_instance.service_type,
                    collector_instance.module_name
                )
            if collector_instance.submodule_name:
                collector_unique_identifier = f"{collector_unique_identifier}_{collector_instance.submodule_name}"

            if collector_unique_identifier in PersistenceManager.persistence_instances:
                persistence_instance = self.persistence_instances[collector_unique_identifier]
            else:
                persistence_instance = \
                    self.persistence_manager_instance.get_instance_for_unique_identifier(collector_unique_identifier)
                PersistenceManager.persistence_instances[collector_unique_identifier] = persistence_instance
            return persistence_instance

    def get_instance_for_unique_id(self, unique_identifier: str):
        """

        :param unique_identifier:
        :return:
        """

        if self.persistence_manager_instance:
            if unique_identifier in PersistenceManager.persistence_instances:
                persistence_instance = self.persistence_instances[unique_identifier]
            else:
                persistence_instance = \
                    self.persistence_manager_instance.get_instance_for_unique_identifier(unique_identifier)
                PersistenceManager.persistence_instances[unique_identifier] = persistence_instance
            return persistence_instance

    def __str__(self):
        return \
            f'{{"persistence_manager": {self.persistence_manager_instance}}}'
