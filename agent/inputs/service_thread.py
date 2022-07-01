import logging
import sys
import threading
from threading import Thread
from typing import Optional, Union

from agent.commons.collector_utils import CollectorUtils
from agent.commons.non_active_reasons import NonActiveReasons
from agent.inputs.exceptions.exceptions import ServiceCreationException
# noinspection PyUnresolvedReferences
from agent.modules import *
from agent.persistence.persistence_manager import PersistenceManager
from agent.queues.collector_multiprocessing_queue import CollectorMultiprocessingQueue
from agent.queues.collector_multithreading_queue import CollectorMultithreadingQueue

log = logging.getLogger(__name__)

QueueType = Union[CollectorMultiprocessingQueue, CollectorMultithreadingQueue]


class ServiceThread(Thread):

    def __init__(self,
                 parent_thread,
                 input_id: str,
                 input_name: str,
                 input_config: dict,
                 input_definition: dict,
                 input_wait_object: threading.Event,
                 service_name: str,
                 service_type: str,
                 service_config: dict,
                 service_definition: dict,
                 service_globals: dict,
                 persistence_manager: PersistenceManager,
                 standard_queue: QueueType,
                 lookup_queue: QueueType,
                 internal_queue: QueueType) -> None:
        super().__init__()
        self.name = \
            self.__class__.__name__ + \
            f"({input_name},{input_id},{service_name},{service_type})"
        self.debug: bool = bool(input_config.get("debug"))
        if self.debug:
            log.setLevel(logging.DEBUG)

        self.parent_thread = parent_thread

        self.input_id: str = input_id
        self.input_name: str = input_name
        self.input_config: dict = input_config
        self.input_definition: dict = input_definition
        self.input_wait_object: threading.Event = input_wait_object

        self.service_name: str = service_name
        self.service_type: str = service_type
        self.service_config: dict = service_config
        self.service_definition: dict = service_definition
        self.service_globals: dict = service_globals

        self.persistence_manager: PersistenceManager = persistence_manager

        self.standard_queue: QueueType = standard_queue
        self.lookup_queue: QueueType = lookup_queue
        self.internal_queue: QueueType = internal_queue

        self.service_thread_globals = input_definition.get("service_thread_globals")

        self.requests_limits = None
        self.service_thread_execution_periods_in_seconds: int = 600

        if self.service_thread_globals:
            self.requests_limits = self.service_thread_globals.get("requests_limits")
            self.service_thread_execution_periods_in_seconds = \
                self.service_thread_globals.get("service_thread_execution_periods_in_seconds")

        self.module_threads: dict = {}

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

        self.__running_flag: bool = self._create_module_threads()

    def _create_module_threads(self) -> bool:
        """

        :return:
        """
        submodules_instantiated = 0
        total_submodules_amount = 0
        if self.service_definition is None:
            raise ServiceCreationException(
                0, f"Service definition does not exists"
            )
        else:
            modules = self.service_definition.get("modules")
            module_type_mandatory = self.service_definition.get("module_type_mandatory", False)
            if modules is None:
                raise ServiceCreationException(
                    1, f"The definition for \"{self.service_name}\" service should have at least on module definition"
                )
            else:
                for module_name, module_definition in modules.items():
                    module_type: str = module_definition.get("type")
                    module_subtype: str = module_definition.get("subtype")
                    module_default: bool = module_definition.get("default", True)
                    module_enabled: bool = False
                    module_config: Optional[dict] = None
                    if module_type is None:
                        raise ServiceCreationException(
                            2, f"Property \"type\" is missing in \"{module_name}\" module definition"
                        )
                    else:
                        if self.service_config is None and module_default is True:
                            module_enabled = True
                        else:
                            types_entries_from_config = self.service_config.get("types")
                            if module_type_mandatory and types_entries_from_config is None:
                                raise ServiceCreationException(
                                    3,
                                    f'Configuration for service "{self.service_name}" must contain the entry "types"'
                                )
                            if types_entries_from_config is None:
                                if module_default is True:
                                    module_enabled = True
                            else:
                                if isinstance(types_entries_from_config, list):
                                    for type_entry in types_entries_from_config:
                                        if isinstance(type_entry, str):
                                            if type_entry == module_type:
                                                module_enabled = True
                                                break
                                        elif isinstance(type_entry, dict):
                                            if len(type_entry) == 1:
                                                type_entry_key = list(type_entry.keys())[0]
                                                if type_entry_key == module_type:
                                                    type_entry_value = list(type_entry.values())[0]
                                                    if isinstance(type_entry_value, dict):
                                                        subtype_from_config = type_entry_value.get("subtype")
                                                        if subtype_from_config:
                                                            if subtype_from_config == module_subtype:
                                                                module_enabled = True
                                                                module_config = type_entry_value
                                                                break
                                                        else:
                                                            module_enabled = True
                                                            module_config = type_entry_value
                                                            break
                                                    else:
                                                        if log.isEnabledFor(logging.ERROR):
                                                            log_message = f'Property "{type_entry_key}" from "types" ' \
                                                                          f'is having a wrong format ' \
                                                                          f'({type(type_entry_value)}), ' \
                                                                          f'only is supported "dict"'
                                                            log.error(log_message)
                                                            self.send_internal_collector_message(
                                                                log_message, level="error"
                                                            )
                                            else:
                                                if log.isEnabledFor(logging.ERROR):
                                                    log_message = \
                                                        f"Property \"types\" is having a wrong format, " \
                                                        f"only is supported one unique key entry"
                                                    log.error(log_message)
                                                    self.send_internal_collector_message(log_message, level="error")
                                        else:
                                            if log.isEnabledFor(logging.ERROR):
                                                log_message = \
                                                    f"Property \"types\" is having a wrong format, " \
                                                    f"each entry can be only \"str\" or \"dict\" type"
                                                log.error(log_message)
                                                self.send_internal_collector_message(log_message, level="error")
                                else:
                                    if log.isEnabledFor(logging.ERROR):
                                        log_message = f"Property \"types\" is having a wrong format " \
                                                      f"({type(types_entries_from_config)}), " \
                                                      f"only the \"list\" type is supported"
                                        log.error(log_message)
                                        self.send_internal_collector_message(log_message, level="error")
                                    break

                        if module_enabled is False:
                            if log.isEnabledFor(logging.DEBUG):
                                log_message = f"Module \"{module_name}\" is not enabled"
                                log.debug(log_message)
                                self.send_internal_collector_message(log_message, level="debug")
                        else:
                            submodules_property = module_definition.get("submodules_property")
                            submodules_calculator_class_name = module_definition.get("submodule_calculator_class")
                            submodules: dict = {}
                            if submodules_calculator_class_name:
                                module_class = globals()[submodules_calculator_class_name]
                                try:
                                    submodules = \
                                        module_class.calculate_submodules(
                                            self.service_name,
                                            submodules_property,
                                            self.input_config
                                        )
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

                            elif submodules_property:
                                if submodules_property in self.service_config:
                                    if isinstance(self.service_config[submodules_property], dict):
                                        for submodule_name, submodule_value in self.service_config[
                                                submodules_property].items():
                                            submodules[submodule_name] = submodule_value
                                    elif isinstance(self.service_config[submodules_property], list):
                                        for submodule_value in self.service_config[submodules_property]:
                                            if isinstance(submodule_value, str):
                                                submodules[submodule_value] = submodule_value
                                            elif isinstance(submodule_value, dict) and len(submodule_value) == 1:
                                                submodules[list(submodule_value.keys())[0]] = \
                                                    list(submodule_value.values())[0]
                                            else:
                                                if log.isEnabledFor(logging.ERROR):
                                                    log_message = \
                                                        f"Submodules definition is not having expected structure"
                                                    log.error(log_message)
                                                    self.send_internal_collector_message(log_message, level="error")
                                else:
                                    submodules["all"] = "all"
                                    if log.isEnabledFor(logging.INFO):
                                        log_message = \
                                            f'There is not defined any submodule, ' \
                                            f'using the default one with value "none"'
                                        log.info(log_message)
                                        self.send_internal_collector_message(log_message, level="info")

                            total_submodules_amount += len(submodules)

                            if submodules_property:
                                if len(submodules) > 0:
                                    for submodule_name, submodule_value in submodules.items():
                                        try:
                                            module_class = globals()[module_name]
                                            module_thread = module_class(
                                                self,
                                                self.input_id,
                                                self.input_name,
                                                self.input_config,
                                                self.input_definition,
                                                self.service_name,
                                                self.service_type,
                                                self.service_config,
                                                self.service_definition,
                                                module_name,
                                                module_config,
                                                module_definition,
                                                self.persistence_manager,
                                                self.standard_queue,
                                                self.lookup_queue,
                                                self.internal_queue,
                                                submodule_name,
                                                submodule_value
                                            )
                                            if log.isEnabledFor(logging.DEBUG):
                                                log_message = \
                                                    f"{self.getName()} -> {module_thread.getName()} - " \
                                                    f"Instance created"
                                                log.debug(log_message)
                                                self.send_internal_collector_message(log_message, level="debug")
                                            self.module_threads[module_thread.getName()] = module_thread
                                            submodules_instantiated += 1
                                        except Exception as ex:
                                            if log.isEnabledFor(logging.ERROR):
                                                log_message = f"Error when creating class, " \
                                                              f"module_name: {module_name}, " \
                                                              f"module_definition: {module_definition}, " \
                                                              f"details: {ex}"
                                                log.error(log_message)
                                                self.send_internal_collector_message(log_message, level="error")
                                else:
                                    if log.isEnabledFor(logging.ERROR):
                                        log_message = \
                                            f"Error when creating class, module_name: {module_name}, " \
                                            f"module_definition: {module_definition}"
                                        log.error(log_message)
                                        self.send_internal_collector_message(log_message, level="error")
                            else:
                                try:
                                    module_class = globals()[module_name]
                                    module_thread = module_class(
                                        self,
                                        self.input_id,
                                        self.input_name,
                                        self.input_config,
                                        self.input_definition,
                                        self.service_name,
                                        self.service_type,
                                        self.service_config,
                                        self.service_definition,
                                        module_name,
                                        module_config,
                                        module_definition,
                                        self.persistence_manager,
                                        self.standard_queue,
                                        self.lookup_queue,
                                        self.internal_queue
                                    )
                                    if log.isEnabledFor(logging.DEBUG):
                                        log_message = \
                                            f"{self.getName()} -> {module_thread.getName()} - " \
                                            f"Instance created"
                                        log.debug(log_message)
                                        self.send_internal_collector_message(log_message, level="debug")
                                    self.module_threads[module_thread.getName()] = module_thread
                                    submodules_instantiated += 1
                                except Exception as ex:
                                    if log.isEnabledFor(logging.ERROR):
                                        log_message = \
                                            f"Error when creating class, " \
                                            f"module_name: {module_name}, " \
                                            f"module_definition: {module_definition}, " \
                                            f"details: {ex}"
                                        log.error(log_message)
                                        self.send_internal_collector_message(log_message, level="error")
        if total_submodules_amount > submodules_instantiated:
            if log.isEnabledFor(logging.DEBUG):
                log_message = f"{self.getName()} -> Some modules were not created"
                log.debug(log_message)
                self.send_internal_collector_message(log_message, level="debug")
        if submodules_instantiated > 0:
            return True
        return False

    def wake_up(self) -> None:
        """Send a signal to the waiting object of current thread (service thread)

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

                log.debug(f'Entering in wait status')

                called: bool = self.__thread_wait_object.wait(
                    timeout=self.service_thread_execution_periods_in_seconds
                )
                if called is True:
                    self.__thread_wait_object.clear()

                log.debug(f'Waking up from wait status')

            self.__check_if_pause()

            self.__check_if_stop()

            self._check_modules_status()

            if self._no_running_threads():
                self.__running_flag = False

        log.info("Finalizing thread")

        # Blocking the queues that will not be used anymore
        self.lookup_queue.block_input_queue()
        self.standard_queue.block_input_queue()

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
        """ Start descendant modules and then the current thread

        :return:
        """

        log.info(
            f'{self.name} - Starting thread (execution_period={self.service_thread_execution_periods_in_seconds}s)'
        )

        if len(self.module_threads) > 0:
            for module in self.module_threads.values():
                module.start()
        super().start()

    def pause(self, wait: bool = True) -> None:
        """

        :param wait:
        :return:
        """

        if self.__non_active_reason is None:

            log.info(f'{self.name} -> Pausing current thread')

            self.pause_dependencies(wait=False)

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

    def pause_dependencies(self, wait: bool = None) -> None:
        """

        :return:
        """

        if wait is None:
            raise Exception('The parameter "wait" is mandatory')

        log.info(f'{self.name} -> Pausing all dependent threads')

        if len(self.module_threads) > 0:
            for module_thread in self.module_threads.values():
                module_thread.pause(wait=wait)

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

        if len(self.module_threads) > 0:
            for module_thread in self.module_threads.values():
                if isinstance(module_thread, str) is False:
                    module_thread.stop()

        log.info(f'{self.name} -> All dependent threads have been stopped')

    def _check_modules_status(self) -> None:
        module_threads_to_remove: list = []
        for module_thread_name, module_thread in self.module_threads.items():
            if isinstance(module_thread, str) is False and module_thread.is_alive() is False:
                module_threads_to_remove.append(module_thread_name)

        if len(module_threads_to_remove) > 0:
            for module_thread_name in module_threads_to_remove:
                internal_module_thread_name = self.module_threads[module_thread_name].getName()
                self.module_threads[module_thread_name] = "REMOVED"

                if log.isEnabledFor(logging.INFO):
                    log_message = f"Removed finalized thread: {internal_module_thread_name}"
                    log.info(log_message)
                    self.send_internal_collector_message(log_message, level="info")

    def _no_running_threads(self) -> bool:
        number_of_setup_module_threads = len(self.module_threads)
        number_of_removed_threads = 0
        for module_threads in self.module_threads.values():
            if module_threads == "REMOVED":
                number_of_removed_threads += 1
        if number_of_setup_module_threads == number_of_removed_threads:
            return True
        else:
            if log.isEnabledFor(logging.DEBUG):
                log_message = "Still running {} of {} module threads: {}".format(
                    number_of_setup_module_threads - number_of_removed_threads,
                    number_of_setup_module_threads,
                    [*self.module_threads.keys()]
                )
                log.debug(log_message)
                self.send_internal_collector_message(log_message, level="debug")

            return False

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
            service_name=self.service_name,
            level=level,
            shared_domain=shared_domain
        )
