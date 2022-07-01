import logging
import threading
from threading import Thread
from typing import Union, Optional

from agent.commons.collector_utils import CollectorUtils
from agent.commons.non_active_reasons import NonActiveReasons
from agent.inputs.service_thread import ServiceThread
from agent.persistence.persistence_manager import PersistenceManager
from agent.queues.collector_multiprocessing_queue import CollectorMultiprocessingQueue
from agent.queues.collector_multithreading_queue import CollectorMultithreadingQueue

log = logging.getLogger(__name__)

QueueType = Union[CollectorMultiprocessingQueue, CollectorMultithreadingQueue]


class InputThread(Thread):

    def __init__(self,
                 parent_thread,
                 input_id: str,
                 input_name: str,
                 input_config: dict,
                 input_definition: dict,
                 output_queue: QueueType,
                 lookup_queue: QueueType,
                 internal_queue: QueueType,
                 persistence_manager: PersistenceManager) -> None:
        super().__init__()

        self.name = \
            self.__class__.__name__ + f"({input_name},{input_id})"
        self.debug: bool = bool(input_config.get("debug"))
        if self.debug:
            log.setLevel(logging.DEBUG)

        self.parent_thread = parent_thread

        self.input_id: str = input_id
        self.input_name: str = input_name
        self.input_config: dict = input_config
        self.input_definition: dict = input_definition

        self.output_queue: QueueType = output_queue
        self.lookup_queue: QueueType = lookup_queue
        self.internal_queue: QueueType = internal_queue

        self.input_thread_globals = input_definition.get("input_thread_globals")

        self.requests_limits = None
        self.input_thread_execution_periods_in_seconds: int = 600

        if self.input_thread_globals:
            self.requests_limits = self.input_thread_globals.get("requests_limits")
            self.input_thread_execution_periods_in_seconds = \
                self.input_thread_globals.get("input_thread_execution_periods_in_seconds")

        self.persistence_manager = persistence_manager

        # Variables related to run/pause/stop statuses
        self.__thread_wait_object: threading.Event = threading.Event()
        self.__non_active_reason: Optional[str] = None

        self.__stop_thread: bool = False
        self.__wait_object_for_stop_method: threading.Event = threading.Event()
        self.__thread_is_stopped: bool = False

        self.__pause_thread: bool = False
        self.__wait_object_for_pause_run: threading.Event = threading.Event()
        self.__wait_object_for_pause_method: threading.Event = threading.Event()
        self.__thread_is_paused: bool = False

        self.service_threads: dict = {}
        self.initial_running_flag_state: bool = self.__create_service_threads()
        self.__running_flag: bool = True if self.initial_running_flag_state is True else False

    def __create_service_threads(self) -> bool:
        """

        :return:
        """

        services_available = 0
        services_config = self.input_config.get("services")

        if services_config:
            for service_name, service_config in services_config.items():
                service_definition = self._get_service_definition(service_name)
                service_globals = self.input_definition.get("globals")
                if service_definition and len(service_definition) > 0:
                    service_type = service_definition["type"] if "type" in service_definition else None
                    if service_type:
                        try:
                            service_thread = ServiceThread(
                                self,
                                self.input_id,
                                self.input_name,
                                self.input_config,
                                self.input_definition,
                                self.__thread_wait_object,
                                service_name,
                                service_type,
                                service_config,
                                service_definition,
                                service_globals,
                                self.persistence_manager,
                                self.output_queue,
                                self.lookup_queue,
                                self.internal_queue
                            )
                            if log.isEnabledFor(logging.DEBUG):
                                log_message = \
                                    f"{self.getName()} -> {service_thread.getName()} - " \
                                    f"Instance created"
                                log.debug(log_message)
                                self.send_internal_collector_message(log_message, level="debug")
                            self.service_threads[service_thread.getName()] = service_thread
                            services_available += 1
                        except Exception as ex:
                            if log.isEnabledFor(logging.ERROR):
                                log_message = \
                                    f"{self.getName()} - " \
                                    f"Error when creating service \"{service_name}\": {ex}"
                                log.error(log_message)
                                self.send_internal_collector_message(log_message, level="error")
                    else:
                        if log.isEnabledFor(logging.ERROR):
                            log_message = \
                                f"{self.getName()} - " \
                                f"\"type\" property in service definition is missing"
                            log.error(log_message)
                            self.send_internal_collector_message(log_message, level="error")
                else:
                    if log.isEnabledFor(logging.ERROR):
                        log_message = \
                            f"{self.getName()} - " \
                            f"No service definition found for service name: {service_name}"
                        log.error(log_message)
                        self.send_internal_collector_message(log_message, level="error")
        else:

            log.error(f'{self.getName()} - No service has been defined in inputs_configuration file')

        if services_available > 0:
            return True
        return False

    def wake_up(self) -> None:
        """

        :return:
        """

        if self.__thread_wait_object.is_set() is False:
            self.__thread_wait_object.set()

    def run(self) -> None:
        """

        :return:
        """

        while self.__running_flag:

            if self.__pause_thread is False \
                    and self.__stop_thread is False:

                log.debug("Entering in wait status")

                called: bool = self.__thread_wait_object.wait(
                    timeout=self.input_thread_execution_periods_in_seconds
                )
                if called is True:
                    self.__thread_wait_object.clear()

                log.debug("Waking up from wait status")

            self.__check_if_pause()

            self.__check_if_stop()

            self._check_services_status()

            if self._no_running_threads():
                self.__running_flag = False

        log.info("Finalizing thread")

        self.__thread_is_stopped = True
        if self.__wait_object_for_stop_method.is_set() is False:
            self.__wait_object_for_stop_method.set()

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
            self.__running_flag = False

    def start(self) -> None:
        """

        :return:
        """

        log.info(
            f'{self.name} - Starting thread (execution_period={self.input_thread_execution_periods_in_seconds}s)'
        )

        if len(self.service_threads) > 0:
            for service_thread in self.service_threads.values():
                service_thread.start()
        super().start()

    def pause(self, wait: bool = True) -> None:
        """

        :param wait:
        :return:
        """

        if self.__non_active_reason is None:

            log.info(f'{self.name} -> Pausing current thread')

            self.pause_dependencies()

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
                log.debug(f'{self.name} -> Thread has been paused without waiting phase (async)')

    def pause_dependencies(self) -> None:
        """

        :return:
        """

        log.info(f'{self.name} -> Pausing all dependent threads')

        if len(self.service_threads) > 0:
            for service_thread in self.service_threads.values():
                service_thread: ServiceThread
                service_thread.pause()

        log.info(f'{self.name} -> All dependent threads have been paused')

    def stop(self) -> None:
        """

        :return:
        """

        if self.__thread_is_paused is False:
            print(f"{self.name} -> Is not in pause status")

        log.info(f'{self.name} -> Stopping thread')

        self.__non_active_reason = NonActiveReasons.STOP_COMMAND_RECEIVED
        self.__stop_thread = True

        self.__stop_dependencies()

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

        if len(self.service_threads) > 0:
            for service_thread in self.service_threads.values():
                if isinstance(service_thread, ServiceThread):
                    service_thread.stop()

        log.info(f'{self.name} -> All dependent threads have been stopped')

    def _check_services_status(self) -> None:
        """

        :return:
        """
        service_threads_to_remove: list = []
        for service_thread_name, service_thread in self.service_threads.items():
            if isinstance(service_thread, str) is False and service_thread.is_alive() is False:
                service_threads_to_remove.append(service_thread_name)
        if len(service_threads_to_remove) > 0:
            for service_thread_name in service_threads_to_remove:
                internal_service_thread_name = self.service_threads[service_thread_name].getName()
                self.service_threads[service_thread_name] = "REMOVED"

                log.info(f'Removed finalized thread: {internal_service_thread_name}')

    def _no_running_threads(self) -> bool:
        number_of_setup_service_threads = len(self.service_threads)
        number_of_removed_threads = 0
        for setup_service_threads in self.service_threads.values():
            if setup_service_threads == "REMOVED":
                number_of_removed_threads += 1
        if number_of_setup_service_threads == number_of_removed_threads:
            return True
        else:
            log.debug(
                "Still running {} of {} service threads: {}".format(
                    number_of_setup_service_threads - number_of_removed_threads,
                    number_of_setup_service_threads,
                    [*self.service_threads.keys()]
                )
            )
            return False

    def _get_service_definition(self, service_name: str) -> dict:
        services = self.input_definition.get("services")
        if service_name in services and services[service_name]["type"] == "predefined":
            return services[service_name]
        else:
            if "custom_service" in services:
                return services["custom_service"]

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
            self.internal_queue,
            message_content,
            input_name=self.input_name,
            level=level,
            shared_domain=shared_domain
        )