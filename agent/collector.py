# -*- coding: utf-8 -*-
import logging
import threading
from collections import deque
from queue import Queue
from typing import Union, Optional, Deque

from agent.configuration.configuration import CollectorConfiguration
from agent.controllers.input_controller_thread import InputControllerThread
from agent.controllers.output_controller_thread import OutputControllerThread
from agent.controllers.statuses.component_group_status import ComponentGroupStatus
from agent.controllers.statuses.component_status import ComponentStatus
from agent.controllers.statuses.status_enum import StatusEnum
from agent.queues.collector_multiprocessing_queue import CollectorMultiprocessingQueue
from agent.queues.collector_multithreading_queue import CollectorMultithreadingQueue
from agent.queues.communication_queue import CommunicationQueue
from agent.queues.content.collector_notification import CollectorNotification
from agent.queues.content.collector_order import CollectorOrder
from agent.queues.content.communication_queue_notification import CommunicationQueueNotification

APPNAME = 'devo-collector-base'
FORMAT_DATETIME = '%Y-%m-%d %H:%M:%S.%f'
FLAG_FORMAT = "YYYY-MM-DD HH:MM:SS.nnnnnnnnn"
FLAG_REGEX = r"[0-9]{4}-(?:0[1-9]|1[0-2])-(?:0[1-9]|[1-2][0-9]|3[0-1]) " \
             r"(?:2[0-3]|[01][0-9]):[0-5][0-9](?::[0-5][0-9])?(?:\.\d{1,9})?"

# Global definitions
log = logging.getLogger(__name__)
log_level = logging.INFO

QueueType = Union[CollectorMultiprocessingQueue, CollectorMultithreadingQueue]


class CollectorThread(threading.Thread):
    """
    This class represents the whole collector
    """

    def __init__(self,
                 config: CollectorConfiguration,
                 standard_queue: QueueType,
                 lookup_queue: QueueType,
                 internal_queue: QueueType):
        super().__init__()

        self.name = self.__class__.__name__

        self.__output_command_queue: CommunicationQueue = CommunicationQueue()
        self.__input_command_queue: CommunicationQueue = CommunicationQueue()

        self.__output_controller_thread = \
            OutputControllerThread(
                self,
                config,
                standard_queue,
                lookup_queue,
                internal_queue,
                self.__output_command_queue
            )

        self.__input_controller_thread = \
            InputControllerThread(
                self,
                config,
                standard_queue,
                lookup_queue,
                internal_queue,
                self.__input_command_queue
            )

        self.__standard_queue = standard_queue
        self.__lookup_queue = lookup_queue
        self.__internal_queue = internal_queue

        self.__running_flag = True
        self.__main_wait_object: threading.Event = threading.Event()

        self.__component_status: ComponentStatus = ComponentStatus(self.name)

        self.__component_statuses: ComponentGroupStatus = ComponentGroupStatus(name=self.name)
        self.__component_statuses.add_component_to_group(self.__component_status)

        self.__all_data_inputs_paused: bool = False
        self.__all_data_inputs_totally_flushed: bool = False
        self.__all_senders_flushing_data_to_emergency_persistence_system: bool = False
        self.__all_senders_totally_flushed: bool = False

        self.__orders_queue = Queue()
        self.__notifications_queue: Deque = deque()
        self.__current_order: Optional[CollectorOrder] = None
        self.__already_processed_error_notifications: list = []

    def start(self) -> None:
        """

        :return:
        """

        self.__output_controller_thread.start()
        output_controller_status: ComponentStatus = self.__output_controller_thread.get_status()
        self.__component_statuses.add_component_to_group(output_controller_status)

        self.__input_controller_thread.start()
        input_controller_status: ComponentStatus = self.__input_controller_thread.get_status()
        self.__component_statuses.add_component_to_group(input_controller_status)

        super().start()

    def stop(self) -> None:
        """

        :return:
        """
        self.__running_flag = False

    def run(self) -> None:
        """

        :return:
        """

        while self.__running_flag:

            self.__component_status.status = StatusEnum.RUNNING

            if self.__output_command_queue.has_pending_notification() is False \
                    and self.__input_command_queue.has_pending_notification() is False:
                log.debug("Entering in wait status")
                called: bool = self.__main_wait_object.wait(timeout=60)
                if called is True:
                    self.__main_wait_object.clear()
                log.debug("Waking up from wait status")

            collector_must_exit: bool = self.check_collector_must_exit()
            if collector_must_exit is True:
                self.stop()
                continue

            received_any_notification: bool = False
            # Checks if any notification has been received
            while self.__output_command_queue.has_pending_notification():
                notification: CommunicationQueueNotification = self.__output_command_queue.get_notification()
                log.debug(f'Received notification: {notification}')
                if notification in self.__already_processed_error_notifications:
                    log.debug(f'Discarding already processed notification type, notification: {notification}')
                else:
                    self.__notifications_queue.append(notification)
                    received_any_notification = True

            while self.__input_command_queue.has_pending_notification():
                notification: CommunicationQueueNotification = self.__input_command_queue.get_notification()
                log.debug(f'Received notification: {notification}')
                if notification in self.__already_processed_error_notifications:
                    log.debug(f'Discarding already processed notification type, notification: {notification}')
                else:
                    self.__notifications_queue.append(notification)
                    received_any_notification = True

            if received_any_notification is True:
                # Check if received notification is caused by current order
                if self.__current_order:
                    self.__check_if_notifications_are_valid_for_current_order()
                elif len(self.__notifications_queue) > 0:
                    notification_for_being_used = self.__notifications_queue.popleft()
                    log.debug(
                        f'There is no pending order, '
                        f'so some decisions will be made based on the notification: {notification_for_being_used}'
                    )
                    decisions: list[CollectorOrder] = self.__make_decisions(notification_for_being_used)
                    for index, decision in enumerate(decisions, start=1):
                        log.debug(f'Decision #{index}: {decision}')
                        self.__orders_queue.put(decision)

            if self.__orders_queue.qsize() > 0 and self.__current_order is None:

                self.__current_order = self.__orders_queue.get(block=False)
                log.debug(f'Processing order: {self.__current_order}')

                if self.__current_order.is_output():
                    self.__output_command_queue.send_order(self.__current_order)
                    self.__output_controller_thread.wake_up()

                if self.__current_order.is_input():
                    self.__input_command_queue.send_order(self.__current_order)
                    self.__input_controller_thread.wake_up()

        log.info(f'Finalizing thread')

        self.__standard_queue.close()
        self.__lookup_queue.close()
        self.__internal_queue.close()

    def __check_if_notifications_are_valid_for_current_order(self) -> None:
        """

        :return:
        """

        log.debug(f'There is an order waiting to be finished, id: "{self.__current_order.id}"')
        if len(self.__notifications_queue) == 0:
            log.debug(f'The notification queue is empty')
        else:
            order_completed: bool = False
            if self.__current_order == CollectorOrder.PAUSE_ALL_INPUT_OBJECTS \
                    and CollectorNotification.ALL_INPUT_OBJECTS_ARE_PAUSED in self.__notifications_queue:

                self.__notifications_queue.remove(CollectorNotification.ALL_INPUT_OBJECTS_ARE_PAUSED)
                order_completed = True

            elif self.__current_order == CollectorOrder.STOP_ALL_INPUT_OBJECTS \
                    and CollectorNotification.ALL_INPUT_OBJECTS_ARE_STOPPED in self.__notifications_queue:

                self.__notifications_queue.remove(CollectorNotification.ALL_INPUT_OBJECTS_ARE_STOPPED)
                order_completed = True

            elif self.__current_order == CollectorOrder.STOP_INPUT_CONTROLLER \
                    and CollectorNotification.INPUT_PROCESS_IS_STOPPED in self.__notifications_queue:

                self.__notifications_queue.remove(CollectorNotification.INPUT_PROCESS_IS_STOPPED)
                self.__input_controller_thread.stop()
                order_completed = True

            elif self.__current_order == CollectorOrder.PAUSE_ALL_SENDER_MANAGERS \
                    and CollectorNotification.ALL_SENDER_MANAGERS_ARE_PAUSED in self.__notifications_queue:

                self.__notifications_queue.remove(CollectorNotification.ALL_SENDER_MANAGERS_ARE_PAUSED)
                order_completed = True

            elif self.__current_order == CollectorOrder.PAUSE_ALL_CONSUMERS \
                    and CollectorNotification.ALL_CONSUMERS_ARE_PAUSED in self.__notifications_queue:

                self.__notifications_queue.remove(CollectorNotification.ALL_CONSUMERS_ARE_PAUSED)
                order_completed = True

            elif self.__current_order == CollectorOrder.FLUSH_ALL_SENDER_MANAGERS \
                    and CollectorNotification.ALL_SENDER_MANAGERS_ARE_FLUSHED in self.__notifications_queue:

                self.__notifications_queue.remove(CollectorNotification.ALL_SENDER_MANAGERS_ARE_FLUSHED)
                order_completed = True

            elif self.__current_order == CollectorOrder.FLUSH_ALL_CONSUMERS \
                    and CollectorNotification.ALL_CONSUMERS_ARE_FLUSHED in self.__notifications_queue:

                self.__notifications_queue.remove(CollectorNotification.ALL_CONSUMERS_ARE_FLUSHED)
                order_completed = True

            elif self.__current_order == CollectorOrder.STOP_OUTPUT_CONTROLLER \
                    and CollectorNotification.OUTPUT_PROCESS_IS_STOPPED in self.__notifications_queue:

                self.__notifications_queue.remove(CollectorNotification.OUTPUT_PROCESS_IS_STOPPED)
                self.__output_controller_thread.stop()
                order_completed = True

            else:
                log.debug(f'The current order will not use the received notification')

            if order_completed is True:
                log.debug(f'Order completely executed: {self.__current_order}')
                self.__current_order = None

    def check_collector_must_exit(self) -> bool:
        """

        :return:
        """
        if self.__input_controller_thread.is_alive() is False \
                and self.__output_controller_thread.is_alive() is False:
            return True

    def wake_up(self) -> None:
        """Forces the exit from the wait state

        :return:
        """
        if self.__main_wait_object.is_set() is False:
            self.__main_wait_object.set()

    def __make_decisions(self, notification: CollectorNotification) -> list:

        decisions: list[CollectorOrder] = []

        if notification == CollectorNotification.FINAL_SENDER_IS_NOT_WORKING:

            if notification not in self.__already_processed_error_notifications:
                self.__already_processed_error_notifications.append(notification)

            log.info(
                f'It has been received a notification that will cause a '
                f'controlled stop of the whole collector: {notification}')

            decisions.append(
                CollectorOrder.create_pause_all_inputs_order(
                    f'One final sender is failing so all the inputs will be stopped, '
                    f'details: {notification.details}'
                )
            )
            decisions.append(
                CollectorOrder.create_pause_all_sender_managers_order(
                    f'One final sender is failing so all sender managers will be stopped, '
                    f'details: {notification.details}'
                )
            )
            decisions.append(
                CollectorOrder.create_pause_all_consumers_order(
                    f'One final sender is failing so all consumers will be stopped, '
                    f'details: {notification.details}'
                )
            )
            decisions.append(
                CollectorOrder.create_flush_all_sender_managers_order(
                    f'One final sender is failing so all sender managers will be flushed, '
                    f'details: {notification.details}'
                )
            )
            decisions.append(
                CollectorOrder.create_flush_all_consumers_order(
                    f'One final sender is failing so all consumers will be flushed, '
                    f'details: {notification.details}'
                )
            )
            decisions.append(
                CollectorOrder.create_stop_input_controller_order(
                    f'One final sender is failing so all the inputs will be stopped, '
                    f'details: {notification.details}'
                )
            )
            decisions.append(
                CollectorOrder.create_stop_output_controller_order(
                    f'One final sender is failing so all the inputs will be stopped, '
                    f'details: {notification.details}'
                )
            )

        else:
            log.warning(
                f"The notification received will not generate any decision, "
                f"notification: {notification}"
            )

        return decisions
