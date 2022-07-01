from abc import abstractmethod, ABC


class CollectorPersistenceManager(ABC):

    def __init__(self, collector_id: str, persistence_type: str):
        self.__collector_id: str = collector_id
        self.__persistence_type: str = persistence_type

    @property
    def collector_id(self):
        return self.__collector_id

    @property
    def persistence_type(self):
        return self.__persistence_type

    @abstractmethod
    def get_instance_for_unique_identifier(self, unique_identifier: str):
        raise Exception("Method \"get_instance_for_module\" not implemented")
