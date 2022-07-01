import json
import logging
import sys
import threading
import time
from abc import abstractmethod, ABC
from datetime import datetime, timedelta
from threading import Thread
from typing import Optional, Union, List, Any

import pytz as pytz
from ratelimiter import RateLimiter

from agent.commons.collector_utils import CollectorUtils
from agent.commons.constants import Constants
from agent.commons.non_active_reasons import NonActiveReasons
from agent.inputs.collector_puller_setup_abstract import CollectorPullerSetupAbstract
from agent.message.lookup_job_factory import LookupJobFactory, LookupJob
from agent.message.message import Message
from agent.persistence.persistence_manager import PersistenceManager
from agent.queues.collector_multiprocessing_queue import CollectorMultiprocessingQueue
from agent.queues.collector_multithreading_queue import CollectorMultithreadingQueue

log = logging.getLogger(__name__)

QueueType = Union[CollectorMultiprocessingQueue, CollectorMultithreadingQueue]


class CollectorPullerAbstract(Thread, ABC):

    def __init__(self,
                 parent_thread,
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
                 persistence_manager: PersistenceManager,
                 output_queue: QueueType,
                 lookup_queue: QueueType,
                 internal_queue: QueueType,
                 submodule_name: str = None,
                 submodule_config=None) -> None:
        """

        :param parent_thread:
        :param input_id:
        :param input_name:
        :param input_config:
        :param input_definition:
        :param service_name:
        :param service_type:
        :param service_config:
        :param service_definition:
        :param module_name:
        :param module_config:
        :param module_definition:
        :param persistence_manager:
        :param output_queue:
        :param lookup_queue:
        :param internal_queue:
        :param submodule_name:
        :param submodule_config:
        """

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
            self.__class__.__name__ \
            + f"({input_name},{input_id}," \
              f"{service_name},{service_type}{submodule_id})"

        self.parent_thread = parent_thread

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
        self.submodule_config: Any = submodule_config

        self.output_queue: QueueType = output_queue
        self.lookup_queue: QueueType = lookup_queue
        self.internal_queue: QueueType = internal_queue

        # Persistence
        self.persistence_manager: PersistenceManager = persistence_manager
        self.persistence_object = persistence_manager.get_instance_for_module(self)

        if log.isEnabledFor(logging.DEBUG):
            log_message = f"[INPUT] {self.getName()} - Persistence instance: {self.persistence_object}"
            log.debug(log_message)
            self.send_internal_collector_message(log_message, level="debug")

        # Default values for variables
        self.self_monitoring_tag = \
            f"{Constants.INTERNAL_MESSAGE_TAGGING_BASE}.{self.input_name}.{self.service_name}"
        self.rate_limiter = RateLimiter(max_calls=1000000, period=1, callback=self._limited)
        self.retrieval_offset_in_seconds: float = 0
        self._pre_pull_executed = False
        self.pull_retries: int = 3
        self.setup_creation_timeout_in_second: float = 60
        self.setup_should_exit: bool = False
        self.last_execution_timestamp = None
        self._first_waiting_message = True
        self.first_try_of_data_recovery = True
        self.collector_variables: dict = {}
        self.initialized_variables: bool = False
        self.no_wait_next_loop: bool = False
        self.autosetup_enabled: bool = False
        self.show_puller_loop_flag: bool = True

        # Variables related to run/pause/stop statuses
        self.__thread_wait_object = threading.Event()
        self.__non_active_reason: Optional[str] = None

        self.__stop_thread: bool = False
        self.__wait_object_for_stop_method: threading.Event = threading.Event()
        self.__thread_is_stopped: bool = False

        self.__pause_thread: bool = False
        self.__wait_object_for_pause_run: threading.Event = threading.Event()
        self.__wait_object_for_pause_method: threading.Event = threading.Event()
        self.__thread_is_paused: bool = False

        self.running_flag: bool = True

        self.use_time_slots_for_retrievals = False
        self.request_period_in_seconds: float = 300

        # First load the global definitions that could be overwritten by other definition levels
        self.module_globals = input_definition.get("module_globals")
        if self.module_globals:

            # Setting a value for "use_time_slots_for_retrievals" function from global section
            if "use_time_slots_for_retrievals" in self.module_globals:
                self.use_time_slots_for_retrievals = self.module_globals["use_time_slots_for_retrievals"]

            # Setting a value for "request_period_in_seconds" function from global section
            request_period_in_seconds: float = self.module_globals.get("request_period_in_seconds")
            if request_period_in_seconds and request_period_in_seconds > 0:
                self.request_period_in_seconds = request_period_in_seconds

        if self.input_config:
            # Establish values from configuration file
            requests_per_second: float = self.input_config.get("requests_per_second")
            if requests_per_second and requests_per_second > 0:
                self.collector_variables["requests_per_second"]: float = requests_per_second

            # Setting a value for "Use time slots for retrievals" function from input section
            if "use_time_slots_for_retrievals" in self.input_config:
                self.use_time_slots_for_retrievals = self.input_config["use_time_slots_for_retrievals"]

            autosetup_section = self.input_config.get("autoconfig")
            if autosetup_section:
                self.autosetup_enabled = autosetup_section.get("enabled", False)

        if self.module_definition:
            # Setting a value for "request_period_in_seconds" function from module definition section
            request_period_in_seconds: float = self.module_definition.get("request_period_in_seconds")
            if request_period_in_seconds and request_period_in_seconds > 0:
                self.request_period_in_seconds: float = request_period_in_seconds

            if "setup_class" in self.module_definition and \
                    self.module_definition["setup_class"] is not None \
                    and len(self.module_definition["setup_class"]) > 0:
                self.setup_should_exit = True

            module_properties = self.module_definition.get('module_properties')
            if module_properties:
                self.show_puller_loop_flag = module_properties.get('show_puller_loop_flag')
                if self.show_puller_loop_flag is not False:
                    self.show_puller_loop_flag = True

                requests_per_second: float = module_properties.get("requests_per_second")
                if requests_per_second and requests_per_second > 0:
                    self.collector_variables["requests_per_second"]: float = requests_per_second

        if self.service_config:
            requests_per_second: float = self.service_config.get("requests_per_second")
            if requests_per_second and requests_per_second > 0:
                self.collector_variables["requests_per_second"]: float = requests_per_second

            request_period_in_seconds: float = self.service_config.get("request_period_in_seconds")
            if request_period_in_seconds and request_period_in_seconds > 0:
                self.request_period_in_seconds: float = request_period_in_seconds

            if "retries" in self.service_config:
                self.pull_retries: int = self.service_config["retries"]

            # Setting a value for "Use time slots for retrievals" function from service specific section
            if "use_time_slots_for_retrievals" in self.service_config:
                self.use_time_slots_for_retrievals = self.service_config["use_time_slots_for_retrievals"]

        if self.module_config:
            requests_per_second: float = self.module_config.get("requests_per_second")
            if requests_per_second and requests_per_second > 0:
                self.collector_variables["requests_per_second"]: float = requests_per_second

            # Setting a value for "request_period_in_seconds" function from module config section
            request_period_in_seconds: float = self.module_config.get("request_period_in_seconds")
            if request_period_in_seconds and request_period_in_seconds > 0:
                self.request_period_in_seconds: float = request_period_in_seconds

        if "requests_per_second" in self.collector_variables:
            self.rate_limiter = \
                RateLimiter(
                    max_calls=self.collector_variables["requests_per_second"],
                    period=1,
                    callback=self._limited
                )

        # This method will extract the values for required custom properties
        # from configuration and also from service definitions
        try:
            self.init_variables(
                self.input_config,
                self.input_definition,
                self.service_config,
                self.service_definition,
                self.module_config,
                self.module_definition,
                self.submodule_config
            )
            self.initialized_variables = True

            if log.isEnabledFor(logging.DEBUG):
                log_message = \
                    f'[INPUT] {self.getName()} -> ' \
                    f'All variables have been initialized, ' \
                    f'current "collector_variables": {self.collector_variables}'
                log.debug(log_message)
                self.send_internal_collector_message(log_message, level="debug")

        except Exception as ex:

            if log.isEnabledFor(logging.ERROR):
                log_message = f"{self.getName()} -> Error when initializing the variables, details: {ex}"
                log.error(log_message)
                self.send_internal_collector_message(log_message, level="error")

            self.running_flag = False

        self.setup_instance = self.__create_setup_instance(self.module_definition)
        if self.setup_should_exit and self.setup_instance is None:
            raise Exception("Setup instance should exist but it does not exist")
        # if (datetime.now() - creation_timestamp_start).total_seconds() > self.setup_creation_timeout_in_second:
        #     raise

    def __create_setup_instance(self, module_definition: dict) -> CollectorPullerSetupAbstract:
        """

        :param module_definition:
        :return:
        """

        setup_instance: Optional[CollectorPullerSetupAbstract] = None
        if "setup_class" in module_definition and module_definition["setup_class"]:
            setup_class_name = module_definition.get("setup_class")
            if setup_class_name:
                if self.initialized_variables:
                    try:
                        setup_instance = \
                            self.create_setup_instance(
                                setup_class_name,
                                self.autosetup_enabled,
                                self.collector_variables
                            )

                        if log.isEnabledFor(logging.DEBUG):
                            log_message = f"[SETUP] {self.name} -> {setup_instance.getName()} - Instance created"
                            log.debug(log_message)
                            self.send_internal_collector_message(log_message, level="debug")

                    except Exception as ex:

                        if log.isEnabledFor(logging.ERROR):
                            log_message = f"[SETUP] {self.getName()} - Error when creating class, " \
                                          f"module_name: {self.module_name}, " \
                                          f"module_definition: {module_definition}, " \
                                          f"details: {ex}"
                            log.error(log_message)
                            self.send_internal_collector_message(log_message, level="error")

                else:

                    if log.isEnabledFor(logging.ERROR):
                        log_message = \
                            f"[SETUP] - Setup instance will not be created due there exists " \
                            f"some errors when initializing main variables"
                        log.error(log_message)
                        self.send_internal_collector_message(log_message, level="error")

        return setup_instance

    # Debugging timing request
    def _limited(self, until):
        """

        :param until:
        :return:
        """

        duration = until - time.time()
        threading.current_thread().name = f"{self.getName()} -> Ratelimiter"

        if log.isEnabledFor(logging.DEBUG):
            log_message = F"Sleeping for {duration:.5f} seconds"
            log.debug(log_message)
            self.send_internal_collector_message(log_message, level="debug")

    def run(self) -> None:
        """

        :return:
        """

        retry_counter = self.pull_retries
        retrieving_timestamp_slot = None
        while self.running_flag:

            if self.__pause_thread is False \
                    and self.__stop_thread is False:

                wait_for_configurator_to_be_ready = self.__wait_to_setup_instance_to_be_ready()

                if self.setup_should_exit \
                        and (self.setup_instance is None or self.setup_instance.running_flag is False):
                    self.running_flag = False
                    break

                retrieving_timestamp: datetime = datetime.utcnow().replace(tzinfo=pytz.utc)
                if self._pre_pull_executed is False:
                    try:
                        self.pre_pull(retrieving_timestamp)
                        self._pre_pull_executed = True
                    except Exception as ex:
                        exc_type, exc_value, exc_traceback = sys.exc_info()
                        last_traceback = exc_traceback
                        while last_traceback.tb_next is not None:
                            last_traceback = last_traceback.tb_next

                        if log.isEnabledFor(logging.ERROR):
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
                            log.error(exception)
                            self.send_internal_collector_message(exception, level="error")

                if self.use_time_slots_for_retrievals is True:
                    if retrieving_timestamp_slot is None:
                        now_raw: datetime = datetime.utcnow().replace(tzinfo=pytz.utc)
                        next_retrieval: datetime = \
                            now_raw.replace(microsecond=0) + timedelta(seconds=self.request_period_in_seconds)
                        next_retrieval_epoch: int = int(next_retrieval.timestamp())
                        next_retrieval_epoch = \
                            int(next_retrieval_epoch - (next_retrieval_epoch % self.request_period_in_seconds))
                        wait = next_retrieval_epoch - now_raw.timestamp()

                        if log.isEnabledFor(logging.INFO):
                            log_message = f"Waiting {wait:0.3f} seconds before start pulling"
                            log.info(log_message)
                            self.send_internal_collector_message(log_message, level="info")

                        time.sleep(wait)
                        retrieving_timestamp_slot = \
                            datetime.utcfromtimestamp(next_retrieval_epoch).replace(tzinfo=pytz.utc)
                    else:
                        retrieving_timestamp_slot = \
                            retrieving_timestamp_slot + timedelta(seconds=self.request_period_in_seconds)
                    retrieving_timestamp = retrieving_timestamp_slot

                if wait_for_configurator_to_be_ready is True or \
                        self.first_try_of_data_recovery is True:

                    if log.isEnabledFor(logging.INFO):
                        log_message = \
                            f'Starting data collection every {self.request_period_in_seconds} seconds'
                        log.info(log_message)
                        self.send_internal_collector_message(log_message, level="info")

                    self.first_try_of_data_recovery = False

                try:

                    if log.isEnabledFor(logging.DEBUG):
                        log_messages = [
                            f"Messages in standard output queue: {self.output_queue.qsize()}",
                            f"Messages in lookup output queue: {self.lookup_queue.qsize()}"
                        ]
                        for msg in log_messages:
                            log.debug(msg)
                            self.send_internal_collector_message(msg, level="debug")

                    self.pull(retrieving_timestamp)
                    retry_counter = self.pull_retries
                    self._loop_wait(retrieving_timestamp)

                    if log.isEnabledFor(logging.DEBUG):
                        log_message = "Waking up from wait status"
                        log.debug(log_message)
                        self.send_internal_collector_message(log_message, level="debug")

                except Exception as ex:
                    exc_type, exc_value, exc_traceback = sys.exc_info()
                    last_traceback = exc_traceback
                    while last_traceback.tb_next is not None:
                        last_traceback = last_traceback.tb_next

                    if log.isEnabledFor(logging.ERROR):
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
                        log.error(exception)
                        self.send_internal_collector_message(exception, level="error")

                    if self.setup_instance:
                        self.setup_instance.ready_to_collect = False

                        if log.isEnabledFor(logging.WARNING):
                            log_message = "Waiting until setup thread will execute again"
                            log.warning(log_message)
                            self.send_internal_collector_message(log_message, level="warning")

                        self.setup_instance.wake_up()
                    else:
                        if retry_counter != 0:
                            retry_counter -= 1

                            if log.isEnabledFor(logging.INFO):
                                log_message = \
                                    f'Retrying collector setup. ' \
                                    f'{self.pull_retries - retry_counter}/{self.pull_retries}'
                                log.info(log_message)
                                self.send_internal_collector_message(log_message, level="info")

                            time.sleep(5)
                        else:
                            self.running_flag = False

            self.__check_if_pause()

            self.__check_if_stop()

        log.debug("Finalizing thread")

        self.__thread_is_stopped = True
        if self.__wait_object_for_stop_method.is_set() is False:
            self.__wait_object_for_stop_method.set()

    def __wait_to_setup_instance_to_be_ready(self) -> bool:
        """

        :return:
        """

        # Wait to the setup instance to be ready (if needed)
        wait_for_configurator_to_be_ready = False
        setup_wait_loop_counter = 0
        while self.setup_instance \
                and self.setup_instance.ready_to_collect is False \
                and self.setup_instance.is_paused() is False:
            setup_wait_loop_counter += 1
            if self.setup_instance.running_flag is True:
                if self._first_waiting_message is True:
                    log.warning("Waiting until setup will be executed")
                    self._first_waiting_message = False

                wait_for_configurator_to_be_ready = True

                if setup_wait_loop_counter % 15 == 0:
                    log.warning("Waiting until setup will be executed")

                time.sleep(1)
            else:
                break
        return wait_for_configurator_to_be_ready

    def _loop_wait(self, retrieving_timestamp: datetime):
        """

        :param retrieving_timestamp:
        :return:
        """

        elapsed_time = \
            (datetime.utcnow().replace(tzinfo=pytz.utc) - retrieving_timestamp).total_seconds()
        # Request frequency
        wait_in_seconds = self.request_period_in_seconds
        if retrieving_timestamp:
            wait_in_seconds -= (datetime.utcnow().replace(tzinfo=pytz.utc) - retrieving_timestamp).total_seconds()

        if self.retrieval_offset_in_seconds != 0:
            wait_in_seconds += self.retrieval_offset_in_seconds

            if log.isEnabledFor(logging.INFO):
                log_message = \
                    f"Applied an offset to wait, retrieval_offset: {self.retrieval_offset_in_seconds} seconds"
                log.info(log_message)
                self.send_internal_collector_message(log_message, level="INFO")

            self.retrieval_offset_in_seconds = 0

        if self.no_wait_next_loop is True:

            if log.isEnabledFor(logging.INFO):
                log_message = \
                    f"Elapsed time: {elapsed_time:0.3f} seconds. " \
                    f"It has been activated the flag for no waiting until the next loop"
                log.info(log_message)
                self.send_internal_collector_message(log_message, level="info")

            self.no_wait_next_loop = False
        else:
            if wait_in_seconds > 0:
                # Show show_puller_loop_flag
                if self.show_puller_loop_flag:

                    if log.isEnabledFor(logging.INFO):
                        log_message = \
                            f"Data collection completed. Elapsed time: {elapsed_time:0.3f} seconds. " \
                            f"Waiting for {wait_in_seconds:0.3f} second(s) until the next one"
                        log.info(log_message)
                        self.send_internal_collector_message(log_message, level="info")

                called: bool = self.__thread_wait_object.wait(timeout=wait_in_seconds)
                if called is True:
                    self.__thread_wait_object.clear()
            else:
                # Show show_puller_loop_flag
                if self.show_puller_loop_flag:

                    if log.isEnabledFor(logging.INFO):
                        log_message = \
                            f"Elapsed time: {elapsed_time:0.3f} seconds. " \
                            f"Last retrieval took too much time, no wait will be applied in this loop"
                        log.info(log_message)
                        self.send_internal_collector_message(log_message, level="info")

    def __check_if_pause(self) -> None:
        """

        :return:
        """

        if self.__pause_thread is True:
            log.warning(
                f'Thread has been put in pause status, reason: "{self.__non_active_reason}"'
            )

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

    def wake_up(self) -> None:
        """Force to the wait object to exit from waiting status

        :return:
        """

        if self.__thread_wait_object.is_set() is False:
            self.__thread_wait_object.set()

    def start(self) -> None:
        """

        :return:
        """

        if self.setup_instance:
            self.setup_instance.start()

        log.info(f"{self.name} - Starting thread")

        super().start()

    def pause(self, wait: bool = True) -> None:
        """

        :return:
        """
        if self.__non_active_reason is None:

            log.info(f'{self.name} -> Pausing current thread')

            self.__pause_dependencies()

            self.pull_pause()

            self.__non_active_reason = NonActiveReasons.PAUSE_COMMAND_RECEIVED
            self.__pause_thread = True

            if self.__thread_wait_object.is_set() is False:
                self.__thread_wait_object.set()

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

    def __pause_dependencies(self) -> None:
        """

        :return:
        """

        log.info(f'{self.name} -> Pausing all dependent threads')

        if self.setup_instance and self.setup_instance.is_running():
            self.setup_instance.pause()

        log.info(f'{self.name} -> All dependent threads have been paused')

    def unpause(self) -> None:
        """

        :return:
        """
        self.__pause_thread = False

    def stop(self) -> None:
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

        log.info(f'{self.name} -> Stopping thread')

        self.__non_active_reason = NonActiveReasons.STOP_COMMAND_RECEIVED
        self.__stop_thread = True

        self.__stop_dependencies()

        self.pull_stop()

        if self.__thread_wait_object.is_set() is False:
            self.__thread_wait_object.set()

        if self.__wait_object_for_pause_run.is_set() is False:
            self.__wait_object_for_pause_run.set()

        if self.__wait_object_for_stop_method.is_set() is True:
            self.__wait_object_for_stop_method.clear()

        while self.__thread_is_stopped is False:
            called: bool = self.__wait_object_for_stop_method.wait(timeout=10)
            if called is True:
                self.__wait_object_for_stop_method.clear()

        log.info(f'{self.name} -> Thread has been stopped')

    def __stop_dependencies(self) -> None:
        """

        :return:
        """

        log.info(f'{self.name} -> Stopping all dependent threads')

        if self.setup_instance and self.setup_instance.is_running():
            self.setup_instance.stop()

        log.info(f'{self.name} -> All dependent threads have been stopped')

    def __send_standard_message(self, message: Union[Message, List[Message]]):
        """

        :param message:
        :return:
        """

        self.output_queue.put(message)

    def send_standard_message(self,
                              msg_date: Union[datetime, str, int, float],
                              msg_tag: str,
                              msg_content: Union[dict, str]):
        """
        Method that send an unique message to be consumed by the collector. Only str and dict formats
        are accepted as message payload.
        @param msg_content: an instance of dict or str with the message content
        @param msg_tag: an instance of str with the final devo tag
        @param msg_date: an instance of datetime in UTC
        """

        if isinstance(msg_content, dict):
            msg_content_str = json.dumps(msg_content)
        elif isinstance(msg_content, str):
            msg_content_str = msg_content
        else:
            raise Exception("The message content should be \"dict\" or \"str\" type")

        self.__send_standard_message(Message(msg_date, msg_tag, msg_content_str, is_internal=False))

    def send_standard_messages(self,
                               msg_date: Union[datetime, str, int, float],
                               msg_tag: str,
                               list_of_messages: Union[List[str], List[dict]]):
        """
        Method that send several messages to be consumed by the collector. Only str and dict formats
        are accepted as message payload.
        @param list_of_messages: a list instance of str and dict instances
        @param msg_tag: an instance of str with the final devo tag
        @param msg_date: an instance of datetime in UTC
        """

        messages_ready_to_send: List[Message] = []

        for msg in list_of_messages:
            if isinstance(msg, dict):
                msg_content = json.dumps(msg)
            elif isinstance(msg, str):
                msg_content = msg
            else:
                raise Exception(f'The message content should be <dict> or <str> type not <{type(msg)}>')

            messages_ready_to_send.append(Message(msg_date, msg_tag, msg_content, is_internal=False))

        self.__send_standard_message(messages_ready_to_send)

    def send_lookup_messages(self, lookup_job_factory: LookupJobFactory, start: bool, end: bool, buffer: str):
        """
        Method that creates a lookup_job and sends it to the lookup_queue
        @param lookup_job_factory: A populated lookup_job_factory object.
        @param start: True if the start control should be sent. False if not.
        @param end: True if the start control should be sent. False if not.
        @param buffer: String with the name of the buffer that should be sent to Devo.
            One of these 'Create', 'Modify', 'Remove'.
        """

        buffers = ['Create', 'Modify', 'Remove']

        # Validations
        if not isinstance(lookup_job_factory, LookupJobFactory):
            msg = f'<lookup_job_factory> should be a <LookupJobFactory> instance not -> {type(lookup_job_factory)}'
            raise Exception(f'{msg} {type(lookup_job_factory)}')
        if not isinstance(start, bool):
            msg = f'<start> param should be a <bool> instance not -> {type(start)}'
            raise Exception(f'{msg} {type(start)}')
        if not isinstance(end, bool):
            msg = f'<end> param should be a <bool> instance not -> {type(end)}'
            raise Exception(f'{msg} {type(end)}')
        if buffer not in buffers:
            msg = f'<buffer> param should be one of these {buffers}'
            raise Exception(msg)

        # LookupJob creation
        if buffer == 'Create':
            lookup_job: LookupJob = lookup_job_factory.create_job_to_initialize_items(send_start=start, send_end=end)
        elif buffer == 'Modify':
            lookup_job: LookupJob = lookup_job_factory.create_job_to_modify_items(send_start=start, send_end=end)
        else:
            lookup_job: LookupJob = lookup_job_factory.create_job_to_remove_items(send_start=start, send_end=end)

        # Send lookup job
        self.__send_lookup_job(lookup_job)

    def __send_lookup_job(self, lookup_job: LookupJob):
        """
        Method that puts a lookup_job into lookup_queue.
        @param lookup_job: a LookupJob instance.
        """

        self.lookup_queue.put(lookup_job)

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
    def create_setup_instance(
            self,
            setup_class_name: str,
            autosetup_enabled: bool,
            collector_variables: dict) -> CollectorPullerSetupAbstract:
        """

        :param setup_class_name:
        :param autosetup_enabled:
        :param collector_variables:
        :return:
        """

        pass

    @abstractmethod
    def init_variables(
            self,
            input_config: dict,
            input_definition: dict,
            service_config: dict,
            service_definition: dict,
            module_config: dict,
            module_definition: dict,
            submodule_config: Any) -> None:
        """

        :param input_config:
        :param input_definition:
        :param service_config:
        :param service_definition:
        :param module_config:
        :param module_definition:
        :param submodule_config:
        :return:
        """

        pass

    @abstractmethod
    def pre_pull(self, retrieving_timestamp: datetime) -> None:
        """

        :param retrieving_timestamp:
        :return:
        """

        pass

    @abstractmethod
    def pull(self, retrieving_timestamp: datetime) -> None:
        """

        :param retrieving_timestamp:
        :return:
        """

        pass

    @abstractmethod
    def pull_pause(self) -> None:
        """

        :return:
        """

        pass

    @abstractmethod
    def pull_stop(self) -> None:
        """

        :return:
        """

        pass
