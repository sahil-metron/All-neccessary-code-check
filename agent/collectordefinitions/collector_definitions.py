import os
from typing import Optional

import yaml

from agent.commons.collector_exceptions import CollectorException


class CollectorDefinitions:
    COLLECTOR_DEFINITIONS_FILENAME = "collector_definitions.yaml"

    _collector_globals: Optional[dict] = None
    _collector_inputs: Optional[dict] = None

    @staticmethod
    def get_collector_globals() -> dict:
        if CollectorDefinitions._collector_globals is None:
            CollectorDefinitions._load_collector_definitions()

        return CollectorDefinitions._collector_globals

    @staticmethod
    def get_input_definitions(input_name: str) -> dict:
        if CollectorDefinitions._collector_inputs is None:
            CollectorDefinitions._load_collector_definitions()

        return CollectorDefinitions._collector_inputs.get(input_name)

    @staticmethod
    def _load_collector_definitions() -> None:
        loaded_collector_definitions = False
        with open(
                os.path.join(
                    os.getcwd(),
                    "config_internal",
                    CollectorDefinitions.COLLECTOR_DEFINITIONS_FILENAME
                )
        ) as service_definitions_file:
            service_definitions_content = yaml.safe_load(service_definitions_file)
            if service_definitions_content:
                CollectorDefinitions._collector_globals = service_definitions_content.get("collector_globals")
                CollectorDefinitions._collector_inputs = service_definitions_content.get("collector_inputs")
                loaded_collector_definitions = True
        if loaded_collector_definitions is False:
            raise CollectorException(0, "Collector definitions were not loaded")
