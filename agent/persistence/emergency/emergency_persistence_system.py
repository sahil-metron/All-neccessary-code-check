import logging

from agent.queues.content.collector_queue_item import CollectorQueueItem

log = logging.getLogger(__name__)


class EmergencyPersistenceSystem:

    def __init__(self, output_component_name: str):
        self.__output_component_name: str = output_component_name
        self.__items_persisted: int = 0

    @property
    def output_component_name(self) -> str:
        """

        :return:
        """
        return self.__output_component_name

    @property
    def items_persisted(self) -> int:
        """

        :return:
        """
        return self.__items_persisted

    def send(self, queue_item: CollectorQueueItem) -> None:
        """

        :param queue_item:
        :return:
        """

        self.__items_persisted += 1

        # log.debug(
        #     f'[EMERGENCY PERSISTENCE SYSTEM] {self.__output_component} -> {queue_item}'
        # )

