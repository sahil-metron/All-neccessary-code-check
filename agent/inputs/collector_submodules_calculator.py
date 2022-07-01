from abc import ABC, abstractmethod


class CollectorSubmodulesCalculator(ABC):

    @staticmethod
    @abstractmethod
    def calculate_submodules(service_name: str, submodules_property: dict, input_config: dict):
        pass
