import inspect
import logging
import math
import threading
from abc import ABC, abstractmethod
from datetime import datetime
from queue import Queue
from typing import Union, Mapping, Any, Optional

from agent.commons.non_active_reasons import NonActiveReasons
from agent.controllers.statuses.component_status import ComponentStatus
from agent.controllers.statuses.status_enum import StatusEnum
from agent.message.collector_lookup import CollectorLookup
from agent.message.lookup_job_factory import LookupJob
from agent.message.message import Message
from agent.outputs.senders.stats.sender_message_stats import SenderMessageStats
from agent.outputs.senders.stats.sender_stats import SenderStats
from agent.queues.content.collector_queue_item import CollectorQueueItem
from agent.queues.sender_manager_queue import SenderManagerQueue

SenderType = Union['ConsoleSender', 'DevoSender', 'SyslogSender']
SendersType = Union['ConsoleSenders', 'DevoSenders', 'SyslogSenders']
ManagerMonitorType = Union['ConsoleSenderManagerMonitor', 'DevoSenderManagerMonitor', 'SyslogSenderManagerMonitor']

log = logging.getLogger(__name__)


class SenderAbstract(ABC, threading.Thread):
    """ Sender abstract Class to be used as a base for sender(s) creation """

    def __init__(self, **kwargs: Optional[Any]):
        """Builder

        :param kwargs:  -> group_name (str): Group name
                        -> instance_name (str): Name of the instance
                        -> content_queue (CollectorInternalThreadQueue): Collector Content Queue
                        -> generate_collector_details (bool) Defines if the collector details must be generated
        """
        super().__init__()

        self._validate_kwargs_for_method__init__(kwargs)

        # Kwargs loading
        sender_manager_instance = kwargs['sender_manager_instance']
        group_name: str = kwargs['group_name']
        instance_name: str = kwargs['instance_name']
        content_queue: SenderManagerQueue = kwargs['content_queue']
        generate_collector_details: bool = kwargs.get('generate_collector_details', False)

        self.sender_manager_instance = sender_manager_instance

        # Id settings
        self.internal_name: str = 'SenderAbstract'
        self.group_name: str = group_name
        self.instance_name: str = instance_name

        # Queue settings
        self._content_queue: SenderManagerQueue = content_queue
        self._priority_queue: Queue = Queue()

        # Stats settings
        self._generate_collector_details: bool = generate_collector_details
        self.last_usage_timestamp: Optional[datetime] = None

        # Renaming of thread object
        self.name = self.__class__.__name__ + f"({group_name},{instance_name})"

        # Internal properties
        self._running_flag: bool = True
        self.__flush_to_persistence_system: bool = False
        self.__component_status: ComponentStatus = ComponentStatus(self.name)
        self._non_active_reason: Optional[str] = None

        self.__pause_sender: bool = False
        self.__wait_object_for_pause_run: threading.Event = threading.Event()
        self.__wait_object_for_pause_method: threading.Event = threading.Event()
        self.__wait_object_for_unpause_method: threading.Event = threading.Event()
        self.__sender_is_paused: bool = False

        self.__stop_sender: bool = False
        self.__wait_object_for_stop_method: threading.Event = threading.Event()
        self.__sender_is_stopped: bool = False

        # Sender statistics
        self.__last_stats_shown_timestamp: Optional[datetime] = None
        self.__message_counter: int = 0

        self._sender_stats: SenderStats = SenderStats(self)

    def _validate_kwargs_for_method__init__(self, kwargs: Mapping[str, Any]):
        """Method that raises exceptions when the kwargs are invalid.

        :param kwargs:  -> group_name (str): Group name
                        -> instance_name (str): Name of the instance
                        -> internal_queue (CollectorInternalThreadQueue): Collector Internal Queue
                        -> generate_collector_details (bool) Defines if the collector details must be generated
        :raise: AssertionError and ValueError.
        """
        error_msg = f"[INTERNAL LOGIC] {__class__.__name__}::{inspect.stack()[0][3]} ->"

        # Validate group name

        val_str = 'group_name'
        assert val_str in kwargs, f'{error_msg} The <{val_str}> argument is mandatory.'
        val_value = kwargs[val_str]

        msg = f'{error_msg} The <{val_str}> must be an instance of <str> type not <{type(val_value)}>'
        assert isinstance(val_value, str), msg

        msg = f'{error_msg} The <{val_str}> length must be between 1 and 50 not <{len(val_value)}>'
        assert len(val_value) >= 1, msg
        assert len(val_value) <= 50, msg

        # Validate instance_name

        val_str = 'instance_name'
        assert val_str in kwargs, f'{error_msg} The <{val_str}> argument is mandatory.'
        val_value = kwargs[val_str]

        msg = f'{error_msg} The <{val_str}> must be an instance of <str> type not <{type(val_value)}>'
        assert isinstance(val_value, str), msg

        msg = f'{error_msg} The <{val_str}> length must be between 1 and 50 not <{len(val_value)}>'
        assert len(val_value) >= 1, msg
        assert len(val_value) <= 50, msg

        # Validate content_queue
        val_str = 'content_queue'
        assert val_str in kwargs, f'{error_msg} The <{val_str}> argument is mandatory.'
        val_value = kwargs[val_str]

        msg = f'{error_msg} The <{val_str}> must be an instance of <CollectorInternalThreadQueue> type not ' \
              f'<{type(val_value)}>'
        assert isinstance(val_value, SenderManagerQueue), msg

        # Validate generate_collector_details
        val_str = 'generate_collector_details'
        if val_str in kwargs:
            val_value = kwargs[val_str]

            msg = f'{error_msg} The <{val_str}> must be an instance of <bool> type not <{type(val_value)}>'
            assert isinstance(val_value, bool), msg

    def get_component_status(self) -> ComponentStatus:
        """

        :return:
        """
        return self.__component_status

    def is_in_pause(self) -> bool:
        """

        :return:
        """
        return self.__sender_is_paused

    def is_running(self) -> bool:
        """Method that returns the value of self.running_flag

        :return: True/False
        """
        return self._running_flag

    def internal_queue_size(self) -> int:
        """

        :return:
        """
        return self._content_queue.sending_queue_size()

    def get_max_content_queue_size(self) -> int:
        """

        :return:
        """
        return self._content_queue.maxsize

    def get_stats(self, timestamp: datetime) -> SenderMessageStats:
        """

        :param timestamp:
        :return:
        """
        return self._sender_stats.get_stats_snapshot(timestamp)

    def get_status(self):
        """

        :return:
        """
        return \
            f'{{' \
            f'"internal_queue_size": {self.internal_queue_size()}, ' \
            f'"is_connection_open": {self._get_connection_status()}' \
            f'}}'

    def add_queue_item_to_final_sender(self, queue_item: CollectorQueueItem) -> int:
        """Method that add a new output object to the sender queue

        :param queue_item: Collector Queue Item object
        :return: Internal queue size only if the details are set.
        """

        self._priority_queue.put(queue_item)
        if self._generate_collector_details:
            return self._priority_queue.qsize()

    def run(self) -> None:
        """Method that keeps the code that will be executed by the thread.

        :return:
        """

        self.__last_stats_shown_timestamp = datetime.utcnow()

        while self._running_flag:

            self.__component_status.status = StatusEnum.RUNNING

            self._run_try_catch()

            self.__check_if_pause()

            self.__check_if_stop()

        # The following code will be executed only when sender is going to be terminated
        log.debug("Finalizing thread")
        self._run_finalization()
        self.sender_manager_instance.wake_up()

        self.__sender_is_stopped = True
        self.__component_status.status = StatusEnum.STOPPED

        if self.__wait_object_for_stop_method.is_set() is False:
            self.__wait_object_for_stop_method.set()

        log.info("Thread finalized")

    def __check_if_pause(self) -> None:
        """

        :return:
        """

        if self.__pause_sender is True:

            self.__sender_is_paused = True
            self.__pause_sender = False

            if self._non_active_reason == NonActiveReasons.FINAL_SENDER_IS_NOT_WORKING:
                log_level = logging.WARNING
                self.__component_status.status = StatusEnum.PAUSED_FAILING
            else:
                log_level = logging.INFO
                self.__component_status.status = StatusEnum.PAUSED

            log.log(log_level, f'Sender has been put in pause status, reason: "{self._non_active_reason}"')

            self.sender_manager_instance.wake_up()

            if self.__wait_object_for_pause_method.is_set() is False:
                self.__wait_object_for_pause_method.set()

            called: bool = self.__wait_object_for_pause_run.wait()
            if called is True:
                self.__wait_object_for_pause_run.clear()

            self.__component_status.status = StatusEnum.RUNNING
            self.__sender_is_paused = False

            log.info("Sender has exited from pause status")

    def __check_if_stop(self) -> None:
        """

        :return:
        """

        if self.__stop_sender is True:
            self._running_flag = False
            self.__stop_sender = False

    def _run_execution(self) -> None:
        """Execution code that will be executed by _run_try_catch_structure.

        :return:
        """

        if self._priority_queue.qsize() > 0:
            queue_item: CollectorQueueItem = self._priority_queue.get(timeout=5)
        else:
            queue_item: CollectorQueueItem = self._content_queue.get(timeout=5)

        rejected_queue_item: bool = False
        if queue_item:
            # Check if the metrics should be calculated
            if self._content_queue.is_generating_metrics():
                # Adding enqueued elapsed time to stats object
                self._sender_stats.add_enqueued_elapsed_times(queue_item.elapsed_times())

            # Extract the content
            content = queue_item.content

            # Process the content
            if isinstance(content, list):
                start_time_mark = datetime.utcnow()
                messages_pending_to_be_sent: list[Message] = []
                sender_is_able_to_send: bool = True

                for message in content:
                    message: Message

                    self.__message_counter += 1

                    if sender_is_able_to_send is True:
                        start_time = datetime.utcnow()
                        message_has_been_sent = self._process_message(message, start_time)
                        if message_has_been_sent is False:
                            sender_is_able_to_send = False
                            messages_pending_to_be_sent.append(message)
                    else:
                        messages_pending_to_be_sent.append(message)

                if messages_pending_to_be_sent:
                    self._content_queue.put_rejected(
                        CollectorQueueItem(messages_pending_to_be_sent)
                    )
                    rejected_queue_item = True
                elapsed_time_in_seconds = (datetime.utcnow()-start_time_mark).total_seconds()
                # log.debug(f'[SENDER] Sent {len(content)} messages in {elapsed_time_in_seconds} seconds')
            elif isinstance(content, Message):

                self.__message_counter += 1

                start_time = datetime.utcnow()
                message_has_been_sent = self._process_message(content, start_time)
                if message_has_been_sent is False:
                    self._content_queue.put_rejected(queue_item)
                    rejected_queue_item = True
            elif isinstance(content, LookupJob):
                start_time = datetime.utcnow()
                lookup_has_been_sent = self._process_lookup(content, start_time)
                if lookup_has_been_sent is False:
                    self._content_queue.put_rejected(queue_item)
                    rejected_queue_item = True
            elif isinstance(content, CollectorLookup):
                log.warning(f"Message dropped due this type has been deprecated -> CollectorLookup: {queue_item}")
            else:
                log.error(f"Message dropped (unknown_type): {queue_item}")

        else:
            log.warning('"None" object received')

        # New queue task done!
        self._content_queue.task_done()

        # Manually cleaning of queue message
        del queue_item

        now = datetime.utcnow()
        last_stats_shown_difference_in_seconds = (now - self.__last_stats_shown_timestamp).total_seconds()
        if last_stats_shown_difference_in_seconds > 60:
            self.__last_stats_shown_timestamp = now
            avg_sec = 0
            if self.__message_counter > 0:
                avg_sec = math.floor(self.__message_counter / last_stats_shown_difference_in_seconds)
            log.info(
                f'Consumed messages: {self.__message_counter} messages '
                f'({last_stats_shown_difference_in_seconds} seconds) => {avg_sec} msg/sec'
            )
            self.__message_counter = 0

        if rejected_queue_item is True:
            log.warning(f'Sender has rejected some messages')
            self._non_active_reason = NonActiveReasons.FINAL_SENDER_IS_NOT_WORKING
            self.__pause_sender = True

    def start(self) -> None:
        """

        :return:
        """
        log.info(f"{self.name} -> Starting thread")
        super().start()

    def pause(self) -> None:
        """

        :return:
        """

        if self._non_active_reason is None:
            log.info(f'{self.name} -> Pausing final sender')

            self._non_active_reason = NonActiveReasons.PAUSE_COMMAND_RECEIVED
            self.__pause_sender = True

            while self.__sender_is_paused is False:
                log.debug(f'{self.name} -> Waiting to be paused')
                called: bool = self.__wait_object_for_pause_method.wait(timeout=10)
                if called is True:
                    self.__wait_object_for_pause_method.clear()

            log.debug(f'{self.name} -> Sender has been paused')

    def unpause(self) -> None:
        """

        :return:
        """

        if self.__sender_is_paused is True:
            log.info(f'{self.name} -> Un-pausing final sender')

            self._non_active_reason = None
            self.__pause_sender = False

            if self.__wait_object_for_pause_run.is_set() is False:
                self.__wait_object_for_pause_run.set()

            while self.__sender_is_paused is True:
                log.debug(f'{self.name} -> Waiting to be un-paused')
                called: bool = self.__wait_object_for_unpause_method.wait(timeout=10)
                if called is True:
                    self.__wait_object_for_unpause_method.clear()

            log.debug(f'{self.name} -> Thread has been un-paused')

    def stop(self):
        """

        :return:
        """

        log.info(f'{self.name} -> Stopping thread')

        self._non_active_reason = NonActiveReasons.STOP_COMMAND_RECEIVED
        self.__stop_sender: bool = True

        if self.__sender_is_paused is True and self.__wait_object_for_pause_run.is_set() is False:
            self.__wait_object_for_pause_run.set()

        while self.__sender_is_stopped is False:
            log.debug(f'{self.name} -> Waiting to be stopped')
            called: bool = self.__wait_object_for_stop_method.wait(timeout=10)
            if called is True:
                self.__wait_object_for_stop_method.clear()

        log.info(f'{self.name} -> Thread has been stopped')

    def get_non_active_reason(self) -> str:
        """

        :return:
        """
        return self._non_active_reason

    def send_message_to_emergency_persistence(self, message: Message) -> None:
        """

        :param message:
        :return:
        """
        log_message = \
            f'[Sender][EmergencyPersistence] Persisting message -> {message}'
        log.debug(log_message)

    def send_lookup_to_emergency_persistence(self, lookup: LookupJob) -> None:
        """

        :param lookup:
        :return:
        """
        log_message = \
            f'[Sender][EmergencyPersistence] Persisting lookup -> {lookup}'
        log.debug(log_message)

    @abstractmethod
    def _run_try_catch(self):
        """
        Method that contains the try & catch structure to be abstracted.
        @example:
        try:
            self._run_execution()
        except Empty:
            pass
        except Exception as ex:
            log.error(f'An error has happened, details: {ex}')
            self._running_flag = False
        """
        pass

    @abstractmethod
    def _get_connection_status(self) -> bool:
        """
        Method that returns the sender connection status.
        @return: True if the connection is working. False if not.
        @example:
        is_connection_open = True if self.sender else False
        return is_connection_open
        """
        pass

    @abstractmethod
    def _process_message(self, message: Message, start_time: datetime):
        """
        Method that processes the general types of messages.
        @param message: Message to be processed.
        @param start_time: Time when the process was initialized.
        """
        pass

    @abstractmethod
    def _process_lookup(self, lookup: LookupJob, start_time: datetime):
        """
        Method that processes the lookup type messages.
        @param lookup: Lookup to be processed.
        @param start_time: Time when the process was initialized.
        """
        pass

    @abstractmethod
    def create_sender_if_not_exists(self):
        """ Method that create a senders if not exist """
        pass

    @abstractmethod
    def __str__(self):
        """ Returns a string representation of the class """
        pass

    @abstractmethod
    def _run_finalization(self):
        """
        Method used to execute actions when the <finalizing thread status> is reached
        @example:
        log.info("Finalizing thread")
        if self.sender:
            log.info(f"Closing connection to Syslog server")
            self.sender.close()
            self.sender = None
        """
        pass

    def activate_flush_to_persistence_system_mode(self) -> None:
        """

        :return:
        """

        self.__pause_sender = False
        self.__flush_to_persistence_system = True
        # self.wake_up()
