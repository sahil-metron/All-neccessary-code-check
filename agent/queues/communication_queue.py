from queue import Queue

from agent.queues.content.communication_queue_notification import CommunicationQueueNotification
from agent.queues.content.communication_queue_order import CommunicationQueueOrder


class CommunicationQueue:
    """
    This class will be used for communicating
    """

    def __init__(self) -> None:
        self.__notification_queue: Queue = Queue()
        self.__order_queue: Queue = Queue()

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
        notification = self.__notification_queue.get(block=False)
        self.__notification_queue.task_done()
        return notification

    def has_pending_notification(self) -> bool:
        """

        :return:
        """
        return self.__notification_queue.qsize() > 0

    def send_order(self, order: CommunicationQueueOrder) -> None:
        """

        :param order:
        :return:
        """
        return self.__order_queue.put(order)

    def get_order(self) -> CommunicationQueueOrder:
        """

        :return:
        """
        order = self.__order_queue.get(block=False)
        self.__order_queue.task_done()
        return order

    def has_pending_orders(self) -> bool:
        """

        :return:
        """
        return self.__order_queue.qsize() > 0
