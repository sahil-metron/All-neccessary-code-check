import ctypes
import logging
import multiprocessing
from multiprocessing.connection import Connection
from threading import Thread
from typing import Union

from agent.commons.collector_utils import CollectorUtils
from agent.configuration.configuration import CollectorConfiguration
from agent.controllers.multiprocessing.input_controller_process import InputMultiprocessingController
from agent.controllers.statuses.component_status import ComponentStatus
from agent.controllers.statuses.status_enum import StatusEnum
from agent.queues.collector_multiprocessing_queue import CollectorMultiprocessingQueue
from agent.queues.collector_multithreading_queue import CollectorMultithreadingQueue
from agent.queues.communication_queue import CommunicationQueue
from agent.queues.content.collector_notification import CollectorNotification
from agent.queues.content.communication_queue_order import CommunicationQueueOrder

QueueType = Union[CollectorMultiprocessingQueue, CollectorMultithreadingQueue]

log = logging.getLogger(__name__)


class InputControllerThread(Thread):
    """
    This class will wrap Input process just for simplifying the input management
    """

    def __init__(self,
                 collector_instance,
                 config: CollectorConfiguration,
                 output_queue: QueueType,
                 lookup_queue: QueueType,
                 internal_queue: QueueType,
                 collector_command_queue: CommunicationQueue):
        """

        :param collector_instance:
        :param config:
        :param output_queue:
        :param lookup_queue:
        :param internal_queue:
        :param collector_command_queue:
        """
        super().__init__()

        self.name = self.__class__.__name__

        wait_object: multiprocessing.Event = multiprocessing.Event()
        self.__stop_controller_thread: multiprocessing.Value = multiprocessing.Value(ctypes.c_bool, False)

        # Object to be used as communication channel between Input process and Input thread
        communication_channel_with_input_process, controller_commands_remote = multiprocessing.Pipe()
        communication_channel_with_input_process: Connection
        controller_commands_remote: Connection

        # Input Controller process
        input_controller_process: InputMultiprocessingController = \
            InputMultiprocessingController(
                config,
                output_queue,
                lookup_queue,
                internal_queue,
                controller_thread_wait_object=wait_object,
                controller_commands_connection=controller_commands_remote
            )

        self.__collector_instance = collector_instance
        self.__internal_queue: QueueType = internal_queue
        self.__collector_command_queue: CommunicationQueue = collector_command_queue
        self.__controller_thread_wait_object: multiprocessing.Event = wait_object
        self.__input_controller_process: InputMultiprocessingController = input_controller_process
        self.__controller_commands_local: Connection = communication_channel_with_input_process

        # This variable, most probably, will not be used
        self.__controller_commands_remote: Connection = controller_commands_remote

        self.__running_flag: bool = True

        self.__component_status: ComponentStatus = ComponentStatus(self.name)

    def start(self) -> None:
        """

        :return:
        """
        self.__input_controller_process.start()
        super().start()

    def stop(self) -> None:
        """

        :return:
        """

        self.__running_flag = False
        self.wake_up()

    def run(self) -> None:
        """

        :return:
        """

        while self.__running_flag:

            self.__component_status.status = StatusEnum.RUNNING

            log.debug("Entering in wait status")
            called: bool = self.__controller_thread_wait_object.wait()
            if called:
                self.__controller_thread_wait_object.clear()
            log.debug("Waking up from wait status")

            if self.__stop_controller_thread.value is True:
                self.stop()
                continue

            self.__check_pending_orders()

            self.__check_pending_notifications()

        log.info(f'Finalizing thread, waiting to the dependent process to die')
        self.__input_controller_process.join()

        log.info(f'[EXIT] Process "{self.__input_controller_process.name}" completely terminated')

        self.__collector_instance.wake_up()
        log.info("Thread finalized")

    def __check_pending_notifications(self) -> None:
        """

        :return:
        """

        notification_sent: bool = False
        while self.__controller_commands_local.poll():
            notification: CollectorNotification = self.__controller_commands_local.recv()
            log.debug(
                f'Notification received from InputProcess, '
                f'it will be sent to the CollectorThread: {notification}'
            )
            self.__collector_command_queue.send_notification(notification)
            notification_sent = True

        if notification_sent is True:
            self.__collector_instance.wake_up()

    def __check_pending_orders(self) -> None:
        """

        :return:
        """

        order_sent: bool = False
        while self.__collector_command_queue.has_pending_orders():
            collector_order: CommunicationQueueOrder = self.__collector_command_queue.get_order()

            log.debug(
                f'Order received from CollectorThread, '
                f'it will be sent to the InputProcess: {collector_order}'
            )

            self.__controller_commands_local.send(collector_order)
            order_sent = True

        if order_sent is True:
            self.__input_controller_process.wake_up()

    def wake_up(self) -> None:
        """

        :return:
        """
        if self.__controller_thread_wait_object.is_set() is False:
            self.__controller_thread_wait_object.set()

    def get_status(self) -> ComponentStatus:
        """

        :return:
        """
        return self.__component_status

    def send_internal_collector_message(self,
                                        message_content,
                                        level: str = None,
                                        shared_domain: bool = False) -> None:
        """

        :param message_content:
        :param level:
        :param shared_domain:
        :return:
        """
        CollectorUtils.send_internal_collector_message(
            self.__internal_queue,
            message_content,
            level=level,
            shared_domain=shared_domain
        )
