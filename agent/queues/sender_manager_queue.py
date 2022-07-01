import logging
import threading
from queue import Queue
from typing import Optional

from agent.queues.content.collector_queue_item import CollectorQueueItem

log = logging.getLogger(__name__)


class SenderManagerQueue:
    """
    This queue will be used for passing the messages from the consumer to the final senders
    """

    SENDING: str = "sending"
    REJECTED: str = "rejected"

    def __init__(self,
                 queue_name: str,
                 generate_metrics: bool = False,
                 maxsize: int = 0):

        self.__maxsize: int = maxsize
        self.__queue_name: str = queue_name
        self.__generate_metrics: bool = generate_metrics

        self.__sending_queue: Queue = Queue(maxsize)
        self.__rejected_queue: Queue = Queue(maxsize)
        self.__block_sending_queue: threading.Event = threading.Event()

        # Set the proper "lock" status for not to be activated by default
        self.__block_sending_queue.set()

    @property
    def maxsize(self) -> int:
        """

        :return:
        """
        return self.__maxsize

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

        self.__block_sending_queue.wait()

        self.__sending_queue.put(item, block, timeout)
        if self.__generate_metrics:
            item.enqueued(f'{self.__queue_name}_{self.SENDING}')

        if self.__block_sending_queue.is_set() is False:
            log.debug(f'[QUEUE] There was one item blocked in {self.__queue_name}')

    def get(self,
            block: bool = True,
            timeout: Optional[float] = None) -> CollectorQueueItem:
        """

        :param block:
        :param timeout:
        :return:
        """
        item = self.__sending_queue.get(block, timeout)
        if self.__generate_metrics:
            item.dequeued(f'{self.__queue_name}_{self.SENDING}')
        return item

    def put_rejected(self,
                     item: CollectorQueueItem,
                     block: bool = True,
                     timeout: Optional[float] = None) -> None:
        """

        :param item:
        :param block:
        :param timeout:
        :return:
        """
        self.__rejected_queue.put(item, block, timeout)
        if self.__generate_metrics:
            item.enqueued(f'{self.__queue_name}_{self.REJECTED}')

    def get_rejected(self,
                     block: bool = True,
                     timeout: Optional[float] = None) -> CollectorQueueItem:
        """

        :param block:
        :param timeout:
        :return:
        """
        item = self.__rejected_queue.get(block, timeout)
        if self.__generate_metrics:
            item.dequeued(f'{self.__queue_name}_{self.REJECTED}')
        return item

    def is_generating_metrics(self) -> bool:
        """

        :return:
        """
        return self.__generate_metrics

    def percentage_usage(self) -> float:
        """

        :return:
        """
        return self.__sending_queue.qsize() / self.__sending_queue.maxsize if self.__sending_queue.maxsize else 0

    def sending_queue_size(self) -> int:
        """

        :return:
        """
        return self.__sending_queue.qsize()

    def rejected_queue_size(self) -> int:
        """

        :return:
        """
        return self.__rejected_queue.qsize()

    def task_done(self) -> None:
        """

        :return:
        """
        self.__sending_queue.task_done()

    def rejected_task_done(self) -> None:
        """

        :return:
        """
        self.__rejected_queue.task_done()

    def block_sending_queue(self):
        """

        :return:
        """

        if self.__block_sending_queue.is_set() is True:
            log.info(
                f'{self.__queue_name} - Blocking the input queue for avoiding the receiving of any new incoming message'
            )
            self.__block_sending_queue.clear()

    def release_input_queue(self):
        """

        :return:
        """

        if self.__block_sending_queue.is_set() is False:
            log.info(
                f'{self.__queue_name} - Releasing the sending blocker for continue receiving incoming messages'
            )
            self.__block_sending_queue.set()

    def is_closed_and_empty(self) -> bool:
        """

        :return:
        """
        return self.__block_sending_queue.is_set() is False and self.__sending_queue.qsize() == 0
