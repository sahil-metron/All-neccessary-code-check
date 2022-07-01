from queue import Queue

from agent.queues.content.communication_queue_notification import CommunicationQueueNotification
from agent.queues.content.communication_queue_order import CommunicationQueueOrder


class OneToManyCommunicationQueue:
    """
    This class will be used for communicating
    """

    def __init__(self) -> None:
        self.__notification_queue: Queue = Queue()
        self.__order_queues: dict[str, Queue] = {}

    def send_notification(self, notification: CommunicationQueueNotification) -> None:
        """

        :param notification:
        :return:
        """
        self.__notification_queue.put(notification)

    def get_notification(self) -> CommunicationQueueNotification:
        """

        :return:
        """
        return self.__notification_queue.get()

    def has_pending_notification(self) -> bool:
        """

        :return:
        """
        return self.__notification_queue.qsize() > 0

    def send_order(self, component_name: str, order: CommunicationQueueOrder) -> None:
        """

        :param component_name:
        :param order:
        :return:
        """
        if component_name not in self.__order_queues.keys():
            self.__order_queues[component_name] = Queue()

        self.__order_queues[component_name].put(order)

    def get_order(self, component_name: str) -> CommunicationQueueOrder:
        """

        :return:
        """
        if component_name not in self.__order_queues:
            raise Exception(f'There is no order available for {component_name}')

        return self.__order_queues[component_name].get()

    def has_pending_orders(self, component_name: str) -> bool:
        """

        :return:
        """
        if component_name in self.__order_queues.keys():
            return self.__order_queues[component_name].qsize() > 0
        return False
