from queue import Queue
from typing import Optional

from agent.queues.content.collector_queue_item import CollectorQueueItem


class CollectorInternalThreadQueue(Queue):

    def __init__(self,
                 queue_name: str,
                 generate_metrics: bool,
                 maxsize: int = 0):
        super().__init__(maxsize=maxsize)
        self.__queue_name: str = queue_name
        self.__generate_metrics: bool = generate_metrics

    def put(self,
            item: CollectorQueueItem,
            block: bool = True,
            timeout: Optional[float] = None) -> None:
        """

        :param item:
        :param block:
        :param timeout:
        :return:
        """
        super().put(item, block, timeout)
        if self.__generate_metrics:
            item.enqueued(self.__queue_name)

    def get(self,
            block: bool = True,
            timeout: Optional[float] = None) -> CollectorQueueItem:
        """

        :param block:
        :param timeout:
        :return:
        """
        item = super().get(block, timeout)
        if self.__generate_metrics:
            item.dequeued(self.__queue_name)
        return item

    def is_generating_metrics(self) -> bool:
        """

        :return:
        """
        return self.__generate_metrics

    def __repr__(self) -> str:
        """ String representation of the queue """
        str_representation: str = f'({self.__class__.__name__}) ' \
                                  f'{{' \
                                  f'"queue_name": "{self.__queue_name}", ' \
                                  f'"max_size_in_messages": {self.maxsize}, ' \
                                  f'"generate_metrics": {self.__generate_metrics}' \
                                  f'}}'

        return str_representation

    def percentage_usage(self) -> float:
        """

        :return:
        """
        return self.qsize() / self.maxsize if self.maxsize else 0
