import logging
import sys
import threading
import time
from abc import abstractmethod, ABC
from datetime import datetime
from threading import Thread
from typing import Union, Optional

import pytz

from agent.commons.collector_utils import CollectorUtils
from agent.commons.constants import Constants
from agent.commons.non_active_reasons import NonActiveReasons
from agent.persistence.exceptions.exceptions import ObjectPersistenceException
from agent.queues.collector_multiprocessing_queue import CollectorMultiprocessingQueue
from agent.queues.collector_multithreading_queue import CollectorMultithreadingQueue

log = logging.getLogger(__name__)

QueueType = Union[CollectorMultiprocessingQueue, CollectorMultithreadingQueue]


class CollectorPullerSetupAbstract(Thread, ABC):

    def __init__(self,
                 parent_thread,
                 collector_variables: dict,
                 autosetup_enabled: bool,
                 input_id: str,
                 input_name: str,
                 input_config: dict,
                 input_definition: dict,
                 service_name: str,
                 service_type: str,
                 service_config: dict,
                 service_definition: dict,
                 module_name: str,
                 module_config: dict,
                 module_definition: dict,
                 persistence_object,
                 internal_queue: QueueType,
                 submodule_name: str = None,
                 submodule_config=None) -> None:

        creation_timestamp_start = datetime.now()
        super().__init__()
        self.debug: bool = bool(input_config.get("debug"))
        if self.debug:
            log.setLevel(logging.DEBUG)

        submodule_id = ""
        if isinstance(submodule_config, str):
            submodule_id = f",{submodule_config}"
        elif submodule_name:
            submodule_id = f",{submodule_name}"
        self.name = \
            self.__class__.__name__ + \
            f"({CollectorUtils.collector_name},{input_name}#" \
            f"{input_id},{service_name}#" \
            f"{service_type}{submodule_id})"

        self.parent_thread = parent_thread
        self.collector_variables: dict = collector_variables
        self.autosetup_enabled: bool = autosetup_enabled

        self.input_id: str = input_id
        self.input_name: str = input_name
        self.input_config: dict = input_config
        self.input_definition: dict = input_definition

        self.service_name: str = service_name
        self.service_type: str = service_type
        self.service_config: dict = service_config
        self.service_definition: dict = service_definition

        self.module_name: str = module_name
        self.module_config: dict = module_config
        self.module_definition: dict = module_definition

        self.submodule_name: str = submodule_name
        self.submodule_config = submodule_config

        self.persistence_object = persistence_object

        self.internal_queue: QueueType = internal_queue

        # Variables related to run/pause/stop statuses
        self._thread_wait_object = threading.Event()
        self.__non_active_reason: Optional[str] = None

        self.__stop_thread: bool = False
        self.__wait_object_for_stop_method: threading.Event = threading.Event()
        self.__thread_is_stopped: bool = False

        self.__pause_thread: bool = False
        self.__wait_object_for_pause_run: threading.Event = threading.Event()
        self.__wait_object_for_pause_method: threading.Event = threading.Event()
        self.__thread_is_paused: bool = False

        # Default values for variables
        self.self_monitoring_tag = f"{Constants.INTERNAL_MESSAGE_TAGGING_BASE}.{self.input_name}.{self.service_name}"
        self.rate_limiter = None
        self.refresh_interval_in_seconds: float = 600
        self.setup_retries: int = 3
        self.setup_creation_timeout_in_second: float = 60
        self.last_execution_timestamp = None
        self.ready_to_collect: bool = False
        self.running_flag: bool = True
        self.setup_variables: dict = {}

        # Establish values from configuration file
        setup_section = input_config.get("autoconfig")
        if setup_section:
            if "refresh_interval_in_seconds" in setup_section:
                self.refresh_interval_in_seconds = setup_section["refresh_interval_in_seconds"]
            if "creation_timeout_in_second" in setup_section:
                self.setup_creation_timeout_in_second = setup_section["creation_timeout_in_second"]
            if "retries" in setup_section:
                self.setup_retries = setup_section["retries"]

        # if (datetime.now() - creation_timestamp_start).total_seconds() > self.setup_creation_timeout_in_second:
        #     raise

    # Debugging timing request
    def _limited(self, until):
        """

        :param until:
        :return:
        """

        duration = until - time.time()
        threading.current_thread().name = f"{self.getName()} -> Ratelimiter"

        log.debug(f"Sleeping for {duration:.5f} seconds")

    def run(self) -> None:
        """

        :return:
        """

        retry_counter = self.setup_retries
        while self.running_flag:

            now = datetime.utcnow().replace(tzinfo=pytz.utc)
            try:

                log.debug("Starting loop")

                self._check_persistence()
                self.setup(now)
                self.ready_to_collect = True
                retry_counter = self.setup_retries

                log.info(f"Setup for module \"{self.module_name}\" has been successfully executed")

                self.last_execution_timestamp = now
                self._loop_wait(now)
            except Exception as ex:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                last_traceback = exc_traceback
                while last_traceback.tb_next is not None:
                    last_traceback = last_traceback.tb_next

                exception = {
                    "msg": str(ex),
                    "file": last_traceback.tb_frame.f_code.co_filename,
                    "line": str(last_traceback.tb_lineno),
                    "function": last_traceback.tb_frame.f_code.co_name,
                    "details": {
                        "type": exc_type.__name__,
                        "text": str(exc_value)
                    }
                }
                log.error(str(exception))

                if retry_counter != 0:
                    retry_counter -= 1

                    log.warning(
                        f'Retrying collector setup. {self.setup_retries - retry_counter}/{self.setup_retries}'
                    )
                    time.sleep(5)
                else:
                    self.running_flag = False

            self.__check_if_pause()

            self.__check_if_stop()

        log.info("Finalizing thread")

        self.__thread_is_stopped = True
        if self.__wait_object_for_stop_method.is_set() is False:
            self.__wait_object_for_stop_method.set()

    def _loop_wait(self, retrieving_timestamp: datetime):
        # Request frequency
        wait = self.refresh_interval_in_seconds
        if retrieving_timestamp:
            wait = \
                self.refresh_interval_in_seconds \
                - (datetime.utcnow().replace(tzinfo=pytz.utc) - retrieving_timestamp).total_seconds()
        if wait > 0:

            log.debug(f"Waiting {wait:0.3f} seconds until next setup refresh")

            called: bool = self._thread_wait_object.wait(timeout=wait)
            if called is True:
                self._thread_wait_object.clear()
        else:

            log.info("Last refresh took too much time, no wait will be applied in this loop")

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
            self.running_flag = False

    def is_paused(self) -> bool:
        """

        :return:
        """

        return self.__thread_is_paused

    def is_running(self) -> bool:
        """

        :return:
        """

        return self.running_flag

    def start(self) -> None:
        """

        :return:
        """

        log.info(f"{self.name} -> Starting thread")

        super().start()

    def pause(self, wait: bool = True) -> None:
        """

        :return:
        """

        if self.__non_active_reason is None:

            log.info(f'{self.name} -> Pausing thread')

            self.__non_active_reason = NonActiveReasons.PAUSE_COMMAND_RECEIVED
            self.__pause_thread = True

            if self._thread_wait_object.is_set() is False:
                self._thread_wait_object.set()

            if self.__wait_object_for_pause_method.is_set() is True:
                self.__wait_object_for_pause_method.clear()

            if wait is True:
                while self.__thread_is_paused is False:
                    log.debug(f'{self.name} -> Waiting to be paused')
                    called: bool = self.__wait_object_for_pause_method.wait(timeout=10)
                    if called is True:
                        self.__wait_object_for_pause_method.clear()

                log.info(f'{self.name} -> Thread has been paused after waiting (sync)')
            else:
                log.info(f'{self.name} -> Thread has been paused without waiting phase (async)')

    def stop(self) -> None:
        """

        :return:
        """

        if self.__thread_is_paused is False:
            print(f"{self.name} -> Is not in pause status")

        log.info(f'{self.name} -> Stopping thread')

        self.__non_active_reason = NonActiveReasons.STOP_COMMAND_RECEIVED
        self.__stop_thread = True

        if self._thread_wait_object.is_set() is False:
            self._thread_wait_object.set()

        if self.__wait_object_for_pause_run.is_set() is False:
            self.__wait_object_for_pause_run.set()

        if self.__wait_object_for_stop_method.is_set() is True:
            self.__wait_object_for_stop_method.clear()

        while self.__thread_is_stopped is False:
            called: bool = self.__wait_object_for_stop_method.wait(timeout=10)
            if called is True:
                self.__wait_object_for_stop_method.clear()

        log.info(f'{self.name} -> Thread has been stopped')

    def wake_up(self) -> None:
        """

        :return:
        """
        self._thread_wait_object.set()

    def _check_persistence(self):
        if self.persistence_object:
            persistence_object_is_ok = self.persistence_object.is_ok()
            if persistence_object_is_ok is False:
                raise ObjectPersistenceException(
                    0,
                    "Persistence object is not having a right behavior"
                )

    def send_internal_collector_message(self,
                                        message_content: Union[str, dict],
                                        level: str = None,
                                        shared_domain: bool = False):
        """

        :param message_content:
        :param level:
        :param shared_domain:
        :return:
        """

        # CollectorUtils.send_internal_collector_message(
        #     self.internal_queue,
        #     message_content,
        #     input_name=self.input_name,
        #     service_name=self.service_name,
        #     module_name=self.module_name,
        #     level=level,
        #     shared_domain=shared_domain
        # )
        pass

    def log_error(self, message: str) -> None:
        """

        :param message:
        :return:
        """

        if log.isEnabledFor(logging.ERROR):
            log.error(message)
            self.send_internal_collector_message(message, level="error")

    def log_warning(self, message: str) -> None:
        """

        :param message:
        :return:
        """

        if log.isEnabledFor(logging.WARNING):
            log.warning(message)
            self.send_internal_collector_message(message, level="warning")

    def log_info(self, message: str) -> None:
        """

        :param message:
        :return:
        """

        if log.isEnabledFor(logging.INFO):
            log.info(message)
            self.send_internal_collector_message(message, level="info")

    def log_debug(self, message: str) -> None:
        """

        :param message:
        :return:
        """

        if log.isEnabledFor(logging.DEBUG):
            log.debug(message)
            self.send_internal_collector_message(message, level="debug")

    @abstractmethod
    def setup(self, execution_timestamp: datetime) -> None:
        """

        :param execution_timestamp:
        :return:
        """

        pass
