from queue import Queue
from typing import Any


class EmergencyPersistenceQueue:

    def __init__(self):
        self.__queue = Queue()

    def put(self, queue_item):
        """

        :param queue_item:
        :return:
        """
        self.__queue.put(queue_item)

    def get(self) -> Any:
        """

        :return:
        """
        return self.__queue.get()
