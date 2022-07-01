"""
Consumes internal messages from the queue and sends them to output_internal_senders
"""
import json
import logging
import threading
import time
from datetime import datetime
from queue import Empty
from threading import Thread
from typing import Union, Optional

from agent.commons.non_active_reasons import NonActiveReasons
from agent.controllers.statuses.component_status import ComponentStatus
from agent.controllers.statuses.status_enum import StatusEnum
from agent.outputs.output_sender_manager_list_internal import OutputSenderManagerListInternal
from agent.persistence.emergency.emergency_persistence_system import EmergencyPersistenceSystem
from agent.queues.collector_multiprocessing_queue import CollectorMultiprocessingQueue
from agent.queues.collector_multithreading_queue import CollectorMultithreadingQueue
from agent.queues.content.collector_queue_item import CollectorQueueItem
from agent.queues.one_to_many_communication_queue import OneToManyCommunicationQueue

log = logging.getLogger(__name__)

QueueType = Union[CollectorMultiprocessingQueue, CollectorMultithreadingQueue]


class OutputInternalConsumer(Thread):

    SUPPORTED_QUEUE_ITEMS: [str] = ['standard', 'batch']

    def __init__(self,
                 group_name: str,
                 consumer_identification: int,
                 content_queue: QueueType,
                 sender_manager_list: OutputSenderManagerListInternal,
                 generate_collector_details: bool,
                 output_controller_communication_channel: OneToManyCommunicationQueue = None,
                 output_controller_instance=None):

        super().__init__()
        self.name = self.__class__.__name__ + f"({group_name}_consumer_{consumer_identification})"

        self.output_controller_communication_channel: OneToManyCommunicationQueue = \
            output_controller_communication_channel
        self.output_controller_instance = output_controller_instance

        self.__content_queue: QueueType = content_queue
        self.__sender_manager_list: OutputSenderManagerListInternal = sender_manager_list
        self.__generate_collector_details: bool = generate_collector_details

        self.__running_flag = True
        self.__thread_waiting_object: threading.Event = threading.Event()
        self.__component_status: ComponentStatus = ComponentStatus(self.name)
        self.__non_active_reason: Optional[str] = None

        self.__pause_thread: bool = False
        self.__wait_object_for_pause_run: threading.Event = threading.Event()
        self.__wait_object_for_pause_method: threading.Event = threading.Event()
        self.__wait_object_for_unpause_method: threading.Event = threading.Event()
        self.__thread_is_paused: bool = False

        self.__stop_thread: bool = False
        self.__wait_object_for_stop_method: threading.Event = threading.Event()
        self.__thread_is_stopped: bool = False

        self.__flush_to_persistence_system: bool = False
        self.__wait_object_for_flush_method: threading.Event = threading.Event()
        self.__consumer_has_been_flushed: bool = False

        self.__emergency_persistence_system: EmergencyPersistenceSystem = EmergencyPersistenceSystem(self.name)

    def run(self) -> None:
        """

        :return:
        """
        while self.__running_flag:

            self.__wait_doing_nothing()

            self.__consume_and_send_to_sender_manager_list()

            self.__check_if_pause()

            self.__check_if_flush()

            self.__check_if_stop()

        log.debug(f'Exiting from run method')

        self.__thread_is_stopped = True

        if self.__wait_object_for_stop_method.is_set() is False:
            self.__wait_object_for_stop_method.set()

    def __wait_doing_nothing(self) -> None:
        """

        :return:
        """

        if self.__pause_thread is False \
                and self.__stop_thread is False \
                and self.__flush_to_persistence_system is False \
                and self.__consumer_has_been_flushed is True:

            log.debug("Entering in wait status (doing_nothing)")
            called: bool = self.__thread_waiting_object.wait(timeout=60)
            if called is True:
                self.__thread_waiting_object.clear()
            log.debug("Waking up from wait status (doing_nothing)")

    def __consume_and_send_to_sender_manager_list(self) -> None:
        """

        :return:
        """

        if self.__consumer_has_been_flushed is False:
            start_time_local: Optional[datetime] = None
            try:
                queue_item: CollectorQueueItem = self.__content_queue.get(timeout=5)

                if queue_item:

                    if queue_item.type not in self.SUPPORTED_QUEUE_ITEMS:
                        # Don't process events from not supported queue item types
                        log.warning(
                            f'A queue item has been ignored due to the type {queue_item.type} is not '
                            f'supported by this consumer/queue -> {queue_item}'
                        )
                    else:

                        if self.__generate_collector_details:
                            start_time_local = datetime.utcnow()
                        sender_statuses = self.__sender_manager_list.add_output_object(queue_item)

                        # Forcing the deleting object once it has been used
                        del queue_item

                        if self.__generate_collector_details and sender_statuses:
                            elapsed_seconds_adding_to_sender = \
                                (datetime.utcnow() - start_time_local).total_seconds()
                            if elapsed_seconds_adding_to_sender > 1:
                                log.debug(
                                    f"elapsed_seconds_adding_to_sender: {elapsed_seconds_adding_to_sender}, "
                                    f"{sender_statuses}"
                                )
                else:
                    log.error("'NoneType' object received when retrieving data from the queue")

                # self.__content_queue.task_done()

            except Empty:
                # This exception will be raised when no new message arrives in "timeout" value
                # log.debug(f'The queue {self.__content_queue.queue_name} has no new messages')
                pass

    def __check_if_flush(self) -> None:
        """

        :return:
        """

        if self.__flush_to_persistence_system is True:

            log.info('Flushing thread')

            self.__flush_to_persistence_system = False

            while self.__content_queue.is_closed_and_empty() is False:
                try:
                    queue_item: CollectorQueueItem = self.__content_queue.get(timeout=5)
                    self.__emergency_persistence_system.send(queue_item)
                    # self.__content_queue.task_done()
                except Empty:
                    pass

            self.__consumer_has_been_flushed = True

            log.debug(
                f'[EMERGENCY PERSISTENCE SYSTEM] '
                f'{self.__emergency_persistence_system.output_component_name} '
                f'Persisted {self.__emergency_persistence_system.items_persisted} items ')

            if self.__wait_object_for_flush_method.is_set() is False:
                self.__wait_object_for_flush_method.set()

            log.info(
                f'Thread has been totally flushed '
                f'({self.__emergency_persistence_system.items_persisted})'
            )

    def __check_if_pause(self) -> None:
        """

        :return:
        """

        if self.__pause_thread is True:
            log.warning(
                f'Consumer has been put in pause status, reason: "{self.__non_active_reason}"'
            )
            self.__thread_is_paused = True
            self.__pause_thread = False

            if self.__non_active_reason == NonActiveReasons.FINAL_SENDER_IS_NOT_WORKING:
                self.__component_status.status = StatusEnum.PAUSED_FAILING
            else:
                self.__component_status.status = StatusEnum.PAUSED

            if self.__wait_object_for_pause_method.is_set() is False:
                self.__wait_object_for_pause_method.set()

            called: bool = self.__wait_object_for_pause_run.wait()
            if called is True:
                self.__wait_object_for_pause_run.clear()

            self.__component_status.status = StatusEnum.RUNNING
            self.__thread_is_paused = False

            log.info("Consumer has exited from pause status")

    def __check_if_stop(self) -> None:
        """

        :return:
        """

        if self.__stop_thread is True:
            self.__running_flag = False
            self.__stop_thread = False

    def start(self) -> None:
        """

        :return:
        """
        super().start()

    def stop(self) -> None:
        """

        :return:
        """

        if self.__consumer_has_been_flushed is False:
            log.warning(f"{self.name} -> Has not been totally flushed, waiting until it finish")

            while self.__consumer_has_been_flushed is False:
                log.debug(f'{self.name} -> Waiting to be flushed')
                called: bool = self.__wait_object_for_flush_method.wait(timeout=10)
                if called is True:
                    self.__wait_object_for_flush_method.clear()

        log.debug(f'{self.name} -> Stopping')

        self.__non_active_reason = NonActiveReasons.STOP_COMMAND_RECEIVED
        self.__stop_thread = True

        if self.__thread_waiting_object.is_set() is False:
            self.__thread_waiting_object.set()

        if self.__wait_object_for_pause_run.is_set() is False:
            self.__wait_object_for_pause_run.set()

        if self.__wait_object_for_stop_method.is_set() is True:
            self.__wait_object_for_stop_method.clear()

        while self.__thread_is_stopped is False:
            log.debug(f'{self.name} -> Waiting to be stopped')
            called: bool = self.__wait_object_for_stop_method.wait(timeout=10)
            if called is True:
                self.__wait_object_for_stop_method.clear()

        log.debug(f'{self.name} -> Thread has been stopped')

    def pause(self, wait: bool = True) -> None:
        """

        :return:
        """

        if self.__non_active_reason is None:
            log.info(f'{self.name} -> Pausing consumer')

            self.__non_active_reason = NonActiveReasons.PAUSE_COMMAND_RECEIVED
            self.__pause_thread = True

            if self.__wait_object_for_pause_method.is_set() is True:
                self.__wait_object_for_pause_method.clear()

            if wait is True:
                while self.__thread_is_paused is False:
                    log.debug(f'Waiting to be paused')
                    called: bool = self.__wait_object_for_pause_method.wait(timeout=10)
                    if called is True:
                        self.__wait_object_for_pause_method.clear()

                log.debug(f'{self.name} -> Thread has been paused after waiting (sync)')
            else:
                log.debug(f'{self.name} -> Thread has been paused without waiting phase (async)')

    def flush_to_emergency_persistence_system(self, wait: bool = True) -> None:
        """

        :return:
        """

        if self.__thread_is_paused is False:
            log.warning(f"{self.name} -> Is not in pause status, waiting first to be paused")

            while self.__thread_is_paused is False:
                log.debug(f'{self.name} -> Waiting to be paused')
                called: bool = self.__wait_object_for_pause_method.wait(timeout=10)
                if called is True:
                    self.__wait_object_for_pause_method.clear()

        self.__flush_to_persistence_system = True

        log.info(f'{self.name} -> Enabling flushing mechanism')

        self.__non_active_reason = NonActiveReasons.FLUSH_COMMAND_RECEIVED
        self.__pause_thread = False

        if self.__wait_object_for_pause_run.is_set() is False:
            self.__wait_object_for_pause_run.set()

        # while self.__thread_is_paused is True:
        #     log.debug(f'{self.name} -> Waiting to be un-paused')
        #     called: bool = self.__wait_object_for_unpause_method.wait(timeout=10)
        #     if called is True:
        #         self.__wait_object_for_unpause_method.clear()

        log.debug(f'{self.name} -> Starting to flush the queue')
        if wait is True:

            while self.__consumer_has_been_flushed is False:
                log.debug(f'{self.name} -> Waiting to be flushed')
                called: bool = self.__wait_object_for_flush_method.wait(timeout=10)
                if called is True:
                    self.__wait_object_for_flush_method.clear()

            log.info(f'{self.name} -> Thread has been flushed (sync)')
        else:
            log.debug(f'{self.name} -> Thread has been flushed mode without waiting phase (async)')

    def wake_up(self) -> None:
        """

        :return:
        """

        if self.__thread_waiting_object.is_set() is False:
            self.__thread_waiting_object.set()

    def __str__(self) -> str:
        # consumer_dict: dict = {
        #     "name": self.name,
        #     "running": self.__running_flag,
        #     "senders": self.standard_senders
        # }
        consumer_dict: dict = {
            "name": self.name,
            "running": self.__running_flag
        }
        return json.dumps(consumer_dict)
