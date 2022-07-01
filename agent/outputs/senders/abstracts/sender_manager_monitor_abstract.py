import inspect
import logging
import threading
from abc import ABC, abstractmethod
from datetime import datetime
from threading import Thread
from typing import Union, Any, Mapping, Optional

import pytz

from agent.commons.non_active_reasons import NonActiveReasons
from agent.outputs.senders.stats.sender_message_stats import SenderMessageStats

ManagerType = Union['ConsoleSenderManager', 'DevoSenderManager', 'SyslogSenderManager']
ManagerType_validation = ['ConsoleSenderManager', 'DevoSenderManager', 'SyslogSenderManager']

log = logging.getLogger(__name__)


class SenderManagerMonitorAbstract(ABC, Thread):
    """ Sender Manager Monitor abstract Class to be used as a base """

    def __init__(self, **kwargs: Optional[Any]):
        """
        Builder
        @param kwargs: 	-> manager_to_monitor (ManagerType): SenderManager object to be monitored.
                        -> period_sender_stats_in_seconds (int): Statistics sending period in seconds.
        """
        self._validate_kwargs_for_method__init__(kwargs)

        super().__init__()

        # Kwargs Loading
        manager_to_monitor: ManagerType = kwargs['manager_to_monitor']
        content_type: str = kwargs['content_type']
        period_sender_stats_in_seconds: int = kwargs['period_sender_stats_in_seconds']

        # Id settings
        self.name = self.__class__.__name__ + f"({manager_to_monitor.group_name},{manager_to_monitor.instance_name})"

        # Internal properties
        self._manager_to_monitor: ManagerType = manager_to_monitor
        self._content_type: str = content_type
        self._period_sender_stats_in_seconds: int = period_sender_stats_in_seconds

        # Running properties
        self._running_flag = True
        self.__thread_waiting_object: threading.Event = threading.Event()
        self.__non_active_reason: Optional[str] = None

        self.__pause_thread: bool = False
        self.__wait_object_for_pause_run: threading.Event = threading.Event()
        self.__wait_object_for_pause_method: threading.Event = threading.Event()
        self.__wait_object_for_unpause_method: threading.Event = threading.Event()
        self.__thread_is_paused: bool = False

        self.__stop_thread: bool = False
        self.__wait_object_for_stop_method: threading.Event = threading.Event()
        self.__thread_is_stopped: bool = False


    @staticmethod
    def _validate_kwargs_for_method__init__(kwargs: Mapping[str, Any]):
        """
        Method that raises exceptions when the kwargs are invalid.
        @param kwargs: 	-> manager_to_monitor (ManagerType): SenderManager object to be monitored.
                        -> period_sender_stats_in_seconds (int): Statistics sending period in seconds.
        @raises: AssertionError and ValueError.
        """
        error_msg = f"[INTERNAL LOGIC] {__class__.__name__}::{inspect.stack()[0][3]} ->"

        # Validate manager_to_monitor

        name = 'manager_to_monitor'
        assert name in kwargs, f'{error_msg} The <{name}> argument is mandatory.'
        manager_to_monitor = kwargs[name]

        # To avoid the circular imports this class is using ForwardRef for typed classes. For this reason
        # to complete this validation we can't use the typical isinstance() function.
        msg = f'{error_msg} The <{name}> must be an instance of <ManagerType> type not <{type(manager_to_monitor)}>'
        manager_to_monitor_type: str = str(type(manager_to_monitor)).split('.')[-1][:-2]
        assert manager_to_monitor_type in ManagerType_validation, msg

        # Validate period_sender_stats_in_seconds

        name = 'period_sender_stats_in_seconds'
        assert name in kwargs, f'{error_msg} The <{name}> argument is mandatory.'
        period_stats = kwargs[name]

        msg = f'{error_msg} The <{name}> must be an instance of <int> type not <{type(period_stats)}>'
        assert isinstance(period_stats, int), msg

        msg = f'{error_msg} The <{name}> value must be between 60 and 1800 not <{name}>'
        assert period_stats >= 60, msg
        assert period_stats <= 1800, msg

    def run(self):
        """ Method that contains the thread's execution code """
        while self._running_flag and self._manager_to_monitor and self._manager_to_monitor.is_running():

            # Check if the event object has been called
            called: bool = self.__thread_waiting_object.wait(timeout=self._period_sender_stats_in_seconds)
            if called is True:
                self.__thread_waiting_object.clear()

            if self.__pause_thread is False \
                    and self.__stop_thread is False:
                # Custom hook 01 execution
                self.custom_hook_01()

                # Send information to log
                log.info(
                    f"Number of available senders: {self._manager_to_monitor.senders.get_number_of_instances()}, "
                    f"sender manager internal queue size: {self._manager_to_monitor.internal_queue_size()}"
                )

                # Request the senders' stats
                sender_instances_stats = \
                    self._manager_to_monitor.senders.get_sender_stats(datetime.utcnow().replace(tzinfo=pytz.utc))

                for sender_instance_stats in sender_instances_stats:
                    sender_instance_stats: SenderMessageStats

                    queue_stats_str = sender_instance_stats.get_queue_stats()
                    log.info(queue_stats_str)

                    global_stats_str = sender_instance_stats.get_global_stats()
                    log.info(global_stats_str)

                    # global_message_stats_str = sender_instance_stats.get_global_messages_stats()
                    # log.info(global_message_stats_str)

                    if self._content_type == 'internal':
                        internal_messages_stats_str = sender_instance_stats.get_internal_messages_stats()
                        log.info(internal_messages_stats_str)

                    if self._content_type == 'standard':
                        standard_messages_stats_str = sender_instance_stats.get_standard_messages_stats()
                        log.info(standard_messages_stats_str)

                    if self._content_type == 'lookup':
                        lookup_messages_stats_str = sender_instance_stats.get_lookup_messages_stats()
                        log.info(lookup_messages_stats_str)
            else:
                log.debug(
                    f"{self.name} -> No stats shown due to pending command, "
                    f"pause: {self.__pause_thread}, stop: {self.__stop_thread}"
                )

            self.__check_if_pause()

            self.__check_if_stop()

        self.__thread_is_stopped = True

        if self.__wait_object_for_stop_method.is_set() is False:
            self.__wait_object_for_stop_method.set()

        log.info("Thread finalized")

    def __check_if_pause(self) -> None:
        """

        :return:
        """

        if self.__pause_thread is True:
            log.warning(f'Thread has been put in pause status, reason: "{self.__non_active_reason}"')
            self.__thread_is_paused = True
            self.__pause_thread = False

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
            self._running_flag = False
            self.__stop_thread = False

    def start(self) -> None:
        """ Method that starts the thread execution """
        log.info(f"{self.name} -> Starting thread (every {self._period_sender_stats_in_seconds} seconds)")
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

    def stop(self) -> None:
        """ Method that stops the thread execution """

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

    def wake_up(self) -> None:
        """

        :return:
        """

        if self.__thread_waiting_object.is_set() is False:
            self.__thread_waiting_object.set()

    @abstractmethod
    def custom_hook_01(self):
        """ Abstract method that can be used to implement new functionality. """
        pass
