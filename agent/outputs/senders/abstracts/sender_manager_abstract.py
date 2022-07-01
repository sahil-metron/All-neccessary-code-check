import inspect
import logging
import threading
from abc import ABC, abstractmethod
from datetime import datetime
from queue import Empty
from threading import Thread
from typing import Union, Mapping, Any, Optional

from agent.commons.non_active_reasons import NonActiveReasons
from agent.controllers.statuses.component_status import ComponentStatus
from agent.controllers.statuses.status_enum import StatusEnum
from agent.outputs.exceptions.exceptions import SenderManagerAbstractException
from agent.outputs.senders.abstracts.sender_abstract import SenderAbstract
from agent.outputs.senders.console_sender import ConsoleSender
from agent.outputs.senders.console_sender_manager_monitor import ConsoleSenderManagerMonitor
from agent.outputs.senders.console_senders import ConsoleSenders
from agent.outputs.senders.devo_sender import DevoSender
from agent.outputs.senders.devo_sender_manager_monitor import DevoSenderManagerMonitor
from agent.outputs.senders.devo_senders import DevoSenders
from agent.outputs.senders.syslog_sender import SyslogSender
from agent.outputs.senders.syslog_sender_manager_monitor import SyslogSenderManagerMonitor
from agent.outputs.senders.syslog_senders import SyslogSenders
from agent.persistence.emergency.emergency_persistence_system import EmergencyPersistenceSystem
from agent.queues.content.collector_notification import CollectorNotification
from agent.queues.content.collector_queue_item import CollectorQueueItem
from agent.queues.one_to_many_communication_queue import OneToManyCommunicationQueue
from agent.queues.sender_manager_queue import SenderManagerQueue

SenderType = Union[ConsoleSender, DevoSender, SyslogSender]
SendersType = Union[ConsoleSenders, DevoSenders, SyslogSenders]
ManagerMonitorType = Union[ConsoleSenderManagerMonitor, DevoSenderManagerMonitor, SyslogSenderManagerMonitor]

log = logging.getLogger(__name__)


class SenderManagerAbstract(ABC, Thread):
    """ Sender Manager abstract Class to be used as a base """

    def __init__(self, **kwargs: Optional[Any]):
        """Builder.

        :param kwargs:  -> group_name (str): Group name
                        -> content_type: Type of content [internal|lookup|standard]
                        -> instance_name (str): Name of the instance
                        -> configuration (dict): Dictionary with the configuration
                        -> generate_collector_details (bool) Defines if the collector details must be generated
        """
        super().__init__()

        # Force the initialization of internal vars in the __init__ method
        self._concurrent_connections = None

        self._validate_kwargs_for_method__init__(kwargs)

        # Kwargs loading
        oc_communication_channel: OneToManyCommunicationQueue = kwargs['output_controller_communication_channel']
        oc_instance = kwargs['output_controller_instance']
        group_name: str = kwargs['group_name']
        content_type: str = kwargs['content_type']
        instance_name: str = kwargs['instance_name']
        configuration: dict = kwargs['configuration']
        generate_collector_details: bool = kwargs.get('generate_collector_details', False)

        self._output_controller_communication_channel: OneToManyCommunicationQueue = oc_communication_channel
        self._output_controller_process = oc_instance

        self.content_type = content_type

        # Id settings
        self.internal_name: str = 'SenderManagerAbstract'
        self.group_name: str = group_name
        self.instance_name: str = instance_name

        # Stats settings
        self._generate_collector_details: bool = generate_collector_details

        # Renaming of thread object
        self.name: str = self.__class__.__name__ + f"({group_name},manager,{instance_name})"

        # Settings extraction
        server_configuration: dict = configuration.get("config")
        if not server_configuration:
            raise SenderManagerAbstractException(
                1,
                'Required "config" entry not found in configuration'
            )

        # Request the custom configuration settings
        self._server_configuration(server_configuration)

        # Sender stats and performance
        self._period_sender_stats_in_seconds: int = server_configuration["period_sender_stats_in_seconds"]
        self._generate_metrics: bool = server_configuration["generate_metrics"]
        self._activate_final_queue: bool = server_configuration["activate_final_queue"]

        # Internal properties
        self._sender_manager_queue: SenderManagerQueue = \
            SenderManagerQueue(
                self.name,
                generate_metrics=self._generate_metrics,
                maxsize=10
            )

        # Variables related to run/pause/stop statuses
        self.__running_flag: bool = True
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
        self.__thread_has_been_flushed: bool = False

        # Request the senders dependencies
        self.senders: SendersType = self._senders_constructor()
        self._create_senders(self._get_concurrent_connections())
        self._senders_monitor = self._senders_monitor_constructor()

        self.__emergency_persistence_system: EmergencyPersistenceSystem = EmergencyPersistenceSystem(self.name)

    @property
    def non_active_reason(self) -> str:
        """

        :return:
        """
        return self.__non_active_reason

    @staticmethod
    def _validate_kwargs_for_method__init__(kwargs: Mapping[str, Any]):
        """Method that raises exceptions when the kwargs are invalid.

        :param kwargs:  -> group_name (str): Group name
                        -> instance_name (str): Name of the instance
                        -> configuration (dict): Dictionary with the configuration
                        -> generate_collector_details (bool) Defines if the collector details must be generated
        :raises: AssertionError and ValueError.
        :return:
        """
        error_msg = f"[INTERNAL LOGIC] {__class__.__name__}::{inspect.stack()[0][3]} ->"

        # Validate group_name

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

        # Validate configuration

        val_str = 'configuration'
        assert val_str in kwargs, f'{error_msg} The <{val_str}> argument is mandatory.'
        val_value = kwargs[val_str]

        msg = f'{error_msg} The <{val_str}> must be an instance of <dict> type not <{type(val_value)}>'
        assert isinstance(val_value, dict), msg

        msg = f'{error_msg} The <{val_str}> cannot be empty.'
        assert len(val_value), msg

        # Validate generate_collector_details

        val_str = 'generate_collector_details'
        if val_str in kwargs:
            val_value = kwargs[val_str]

            msg = f'{error_msg} The <{val_str}> must be an instance of <bool> type not <{type(val_value)}>'
            assert isinstance(val_value, bool), msg

    def _get_concurrent_connections(self) -> int:
        """Abstract method that returns the number of concurrent connections that
        will be used by the__create_senders() method.

        :return:
        """
        to_return = 1
        # Return concurrent_connections if defined

        # TODO Commented for having only one sender
        # if 'self._concurrent_connections' in locals():
        #     to_return = self._concurrent_connections

        return to_return

    def _create_senders(self, number_of_sender_instances: int):
        """Method that creates the senders in the senders instance.

        :param number_of_sender_instances: Number of senders to be instantiated.
        :return:
        """

        # Create as many senders as indicated
        for sender_id in range(number_of_sender_instances):
            sender_instance_name = f"{self._build_sender_instance_name(sender_id)}"

            final_sender_queue: SenderManagerQueue = self._sender_manager_queue

            # Instantiate the sender
            self.senders.add_sender(
                self._sender_constructor(
                    final_sender_queue,
                    sender_instance_name,
                    sender_manager_instance=self)
            )

    def add_output_object(self, queue_item: CollectorQueueItem) -> int:
        """ Add a new output object to the sender manager queue """
        self._sender_manager_queue.put(queue_item)
        if self._generate_collector_details:
            return self.internal_queue_size()

    def _create_collector_details(self,
                                  final_sender_queue_size: int,
                                  sender: SenderType,
                                  start_time_local: datetime):
        """Method that creates the collector details when a new item is received.

        :param final_sender_queue_size: Actual number of items (size) in the queue.
        :param sender: Selected sender that will be used to send the queue item.
        :param start_time_local: Start time of the run() method.
        """

        if self._generate_collector_details and final_sender_queue_size:
            elapsed_seconds_adding_to_final_sender = (
                    datetime.utcnow() - start_time_local).total_seconds()
            if final_sender_queue_size > (sender.get_max_content_queue_size() * 0.9) and \
                    elapsed_seconds_adding_to_final_sender > 1:
                log.info(
                    f"elapsed_seconds_adding_to_final_sender: {elapsed_seconds_adding_to_final_sender:0.3f}, "
                    f"final_sender_queue_size: {final_sender_queue_size}, "
                    f"max_final_sender_queue_size: {sender.get_max_content_queue_size()}"
                )

    def internal_queue_size(self) -> int:
        """ Method that returns the size of the sender manager queue """
        return self._sender_manager_queue.sending_queue_size()

    def __str__(self):
        """ Method that returns a str representation of the class """
        return f"{{\"senders\":{self.senders}, \"internal_queue_size\": {self.internal_queue_size()}"

    def start(self) -> None:
        """

        :return:
        """
        """ Method that starts the thread execution """
        self.senders.start_instances()
        self._senders_monitor.start()
        log.info(f"{self.name} -> Starting thread")
        super().start()

    def pause(self) -> None:
        """

        :return:
        """

        if self.__non_active_reason is None:
            log.info(f'{self.name} -> Pausing current thread')

            self.__non_active_reason = NonActiveReasons.PAUSE_COMMAND_RECEIVED
            self.__pause_thread = True

            if self.__thread_waiting_object.is_set() is False:
                self.__thread_waiting_object.set()

            if self.__wait_object_for_pause_method.is_set() is True:
                self.__wait_object_for_pause_method.clear()

            while self.__thread_is_paused is False:
                log.debug(f'{self.name} -> Waiting to be paused')
                called: bool = self.__wait_object_for_pause_method.wait(timeout=10)
                if called is True:
                    self.__wait_object_for_pause_method.clear()

            log.info(f'{self.name} -> Thread has been paused')

    def __pause_dependencies(self) -> None:
        """

        :return:
        """

        log.info(f'{self.name} -> Pausing all dependent threads')

        self.senders.pause_instances()
        self._senders_monitor.pause()

        log.info(f'{self.name} -> All dependent threads have been paused')

    def unpause(self) -> None:
        """

        :return:
        """
        if self.__thread_is_paused is True:
            log.info(f'{self.name} -> Un-pausing')

            self.__non_active_reason = None
            self.__pause_thread = False

            if self.__wait_object_for_pause_run.is_set() is False:
                self.__wait_object_for_pause_run.set()

            while self.__thread_is_paused is True:
                log.debug(f'{self.name} -> Waiting to be un-paused')
                called: bool = self.__wait_object_for_unpause_method.wait(timeout=5)
                if called is True:
                    self.__wait_object_for_unpause_method.clear()

            log.debug(f'{self.name} -> Thread has been un-paused')

    def stop(self) -> None:
        """Method that stops the thread execution

        :return:
        """

        log.info(f'{self.name} -> Stopping thread')

        self.__non_active_reason = NonActiveReasons.STOP_COMMAND_RECEIVED
        self.__stop_thread = True

        if self.__wait_object_for_pause_run.is_set() is False:
            self.__wait_object_for_pause_run.set()

        if self.__thread_waiting_object.is_set() is False:
            self.__thread_waiting_object.set()

        if self.__wait_object_for_stop_method.is_set() is True:
            self.__wait_object_for_stop_method.clear()

        while self.__thread_is_stopped is False:
            log.debug(f'{self.name} -> Waiting to be stopped')
            called: bool = self.__wait_object_for_stop_method.wait(timeout=10)
            if called is True:
                self.__wait_object_for_stop_method.clear()

        log.info(f'{self.name} -> Thread has been stopped')

    def __stop_dependencies(self) -> None:
        """

        :return:
        """

        log.info(f'{self.name} -> Stopping all dependent threads')

        self._senders_monitor.stop()
        self.senders.stop_instances()

        log.info(f'{self.name} -> All dependent threads have been stopped')

    def wake_up(self):
        """Make the thread to exit from "main" waiting status

        :return:
        """

        if self.__thread_waiting_object.is_set() is False:
            self.__thread_waiting_object.set()

    def is_running(self):
        """ Method that returns the status of running_flag """
        return self.__running_flag

    def is_paused(self) -> bool:
        """

        :return:
        """
        return self.__thread_is_paused

    def has_been_flushed(self) -> bool:
        """

        :return:
        """

        return self.__thread_has_been_flushed

    def run(self) -> None:
        """Method that contains the code that will be executed by the thread.

        :return:
        """

        while self.__running_flag:

            self.__component_status.status = StatusEnum.RUNNING

            self.__wait_doing_nothing()

            self.__check_if_pause()

            self.__check_if_flush()

            self.__check_if_stop()

            if self.__running_flag is True:
                self.__check_senders_statuses()

        # If the following code only will be executed if the sender manager is dying
        self.__thread_is_stopped = True
        self.__component_status.status = StatusEnum.STOPPED

        if self.__wait_object_for_stop_method.is_set() is False:
            self.__wait_object_for_stop_method.set()

        log.info("Thread finalized")

    def __wait_doing_nothing(self) -> None:
        """

        :return:
        """

        if self.__pause_thread is False \
                and self.__stop_thread is False \
                and self.__flush_to_persistence_system is False:

            log.debug("Entering in wait status")
            called: bool = self.__thread_waiting_object.wait(timeout=60)
            if called is True:
                self.__thread_waiting_object.clear()
            log.debug("Waking up from wait status")

    def __check_if_flush(self) -> None:
        """

        :return:
        """
        if self.__flush_to_persistence_system is True:

            log.info('Flushing thread')

            self.__flush_to_persistence_system = False

            self._sender_manager_queue.block_sending_queue()
            while self._sender_manager_queue.is_closed_and_empty() is False:
                try:
                    queue_item: CollectorQueueItem = self._sender_manager_queue.get(timeout=5)
                    self.__emergency_persistence_system.send(queue_item)
                    self._sender_manager_queue.task_done()
                except Empty:
                    pass

            self.__thread_has_been_flushed = True

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

            self.__pause_dependencies()

            log.warning(
                f'Thread has been put in pause status, reason: "{self.__non_active_reason}"'
            )
            self.__thread_is_paused = True
            self.__pause_thread = False

            if self.__non_active_reason == NonActiveReasons.FINAL_SENDER_IS_NOT_WORKING:
                self.__component_status.status = StatusEnum.PAUSED_FAILING
                self._output_controller_communication_channel.send_notification(
                    CollectorNotification.create_final_sender_is_not_working_notification(
                        f'{self.name} is failing'
                    )
                )
                self._output_controller_process.wake_up()
            else:
                self.__component_status.status = StatusEnum.PAUSED

            if self.__wait_object_for_pause_method.is_set() is False:
                self.__wait_object_for_pause_method.set()

            called: bool = self.__wait_object_for_pause_run.wait()
            if called is True:
                self.__wait_object_for_pause_run.clear()

            self.__thread_is_paused = False

            log.info("Thread has exited from pause status")

    def __check_if_stop(self) -> None:
        """

        :return:
        """

        if self.__stop_thread is True:
            self.__running_flag = False

            self.__stop_dependencies()
            self.__stop_thread= False

    def __check_senders_statuses(self):
        """

        :return:
        """

        self.__check_if_there_are_rejected_messages()

        pause_sender_manager: bool = False
        most_relevant_non_active_reason: Optional[str] = None
        for sender_instance in self.senders.get_senders():
            sender_instance: SenderAbstract
            non_active_reason = sender_instance.get_non_active_reason()
            if non_active_reason == NonActiveReasons.FINAL_SENDER_IS_NOT_WORKING:
                most_relevant_non_active_reason = non_active_reason
                pause_sender_manager = True
                break
            elif non_active_reason == NonActiveReasons.PAUSE_COMMAND_RECEIVED:
                most_relevant_non_active_reason = non_active_reason
                pause_sender_manager = True

        self.__non_active_reason = most_relevant_non_active_reason
        self.__pause_thread = pause_sender_manager

    def __check_if_there_are_rejected_messages(self):
        """

        :return:
        """

        active_instances: list[SenderAbstract] = self.senders.get_active_senders()
        all_messages_resent: bool = True
        secondary_sender: Optional[SenderAbstract] = None

        if active_instances:
            secondary_sender = active_instances[0]

        while self._sender_manager_queue.rejected_queue_size() > 0:
            try:
                rejected_queue_item: CollectorQueueItem = self._sender_manager_queue.get_rejected(block=False)
                if secondary_sender:
                    # TODO This should never be executed because it supposed that only exists one sender
                    secondary_sender.add_queue_item_to_final_sender(rejected_queue_item)
                else:
                    all_messages_resent = False
                    log.info(
                        f'No other sender is available, so pending content will be sent to '
                        f'the emergency persistence system'
                    )
                    self.__emergency_persistence_system.send(rejected_queue_item)

                # Mark rejected item as processed
                self._sender_manager_queue.rejected_task_done()

            except Empty:
                pass

        if all_messages_resent is False:
            for active_instance in active_instances:
                active_instance.pause()

    @abstractmethod
    def _server_configuration(self, server_configuration: dict):
        """Abstract method where the custom server configuration settings are extracted and stored.

        :param server_configuration: Dict from where the configuration settings are extracted
        :return:
        """
        pass

    @abstractmethod
    def _senders_constructor(self) -> SendersType:
        """Abstract where the senders' object is built.

        :return: An instance of ConsoleSenders, DevoSenders or SyslogSenders.
        """
        pass

    @abstractmethod
    def _senders_monitor_constructor(self) -> ManagerMonitorType:
        """Abstract method where the senders' monitor object is built.

        :return: An instance of ConsoleSenderManagerMonitor, DevoSenderManagerMonitor or SyslogSenderManagerMonitor.
        """
        pass

    @abstractmethod
    def _sender_constructor(self,
                            queue: SenderManagerQueue,
                            sender_name: str,
                            sender_manager_instance=None) -> SenderType:
        """Abstract method where the sender objects are built.

        :param sender_manager_instance: Sender Manager instance for waking up when errors.
        :param queue: QueueType that will be injected to the sender.
        :param sender_name: Name given to the sender instance.
        :return: An instance of ConsoleSender, DevoSender or SyslogSender.
        :rtype: SenderType
        :Example:

            return ConsoleSender(
                self.group_name,
                instance_name,
                self.destination,
                queue,
                self._generate_collector_details
            )
        """
        pass

    @abstractmethod
    def _build_sender_instance_name(self, sender_id: int) -> str:
        """Method that returns the base-name to be used by the senders.

        :param sender_id: Sender number or id. Example: 4
        :return: Base-name in string format. Example: 'console_sender_1'
        :Example:

            return f"my_sender_{sender_id}"
        """
        pass

    def flush_to_emergency_persistence_system(self) -> None:
        """

        :return:
        """

        if self.__thread_is_paused is True:
            log.info(f'{self.name} -> Enabling flushing mechanism')

            self.__non_active_reason = NonActiveReasons.FLUSH_COMMAND_RECEIVED
            self.__flush_to_persistence_system = True

            if self.__wait_object_for_pause_run.is_set() is False:
                self.__wait_object_for_pause_run.set()

            if self.__thread_waiting_object.is_set() is False:
                self.__thread_waiting_object.set()

            log.debug(f'{self.name} -> Starting to flush the queue')

            while self.__thread_has_been_flushed is False:
                log.debug(f'{self.name} -> Waiting to be flushed')
                called: bool = self.__wait_object_for_flush_method.wait(timeout=10)
                if called is True:
                    self.__wait_object_for_flush_method.clear()

            log.debug(f'{self.name} -> Queue has been flushed')
