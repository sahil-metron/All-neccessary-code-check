import ctypes
import gc
import inspect
import json
import logging
import multiprocessing
import os
import threading
from multiprocessing import Process
from multiprocessing.connection import Connection
from typing import Union, Optional

import psutil

from agent.collectordefinitions.collector_definitions import CollectorDefinitions
from agent.commons.collector_utils import CollectorUtils
from agent.configuration.configuration import CollectorConfiguration
from agent.configuration.exceptions import CollectorConfigurationException
from agent.controllers.exceptions.exceptions import InputControllerException
from agent.inputs.input_thread import InputThread
from agent.persistence.exceptions.exceptions import ObjectPersistenceException
from agent.persistence.persistence_manager import PersistenceManager
from agent.queues.collector_multiprocessing_queue import CollectorMultiprocessingQueue
from agent.queues.content.collector_notification import CollectorNotification
from agent.queues.content.collector_order import CollectorOrder

# Globals
log = logging.getLogger(__name__)
log_level = logging.INFO


class InputMultiprocessingController(Process):

    def __init__(self,
                 config: CollectorConfiguration,
                 standard_queue: CollectorMultiprocessingQueue,
                 lookup_queue: CollectorMultiprocessingQueue,
                 internal_queue: CollectorMultiprocessingQueue,
                 controller_thread_wait_object: multiprocessing.Event = None,
                 controller_commands_connection: Connection = None):
        """This class creates all input services in an isolated multiprocessing process.

        It is in charge of puller and setup instances.
        :param config: a CollectorConfiguration object.
        :param standard_queue: An internal multiprocessing queue instance used for sending the standard messages.
        :param lookup_queue: An internal multiprocessing queue instance used for sending the lookup messages.
        :param internal_queue: An internal multiprocessing queue instance used for sending the internal messages.
        :param controller_thread_wait_object: multiprocessing.Event() from parent process
        :param controller_commands_connection:
        """
        super().__init__()

        # Get the class and global properties
        self.name = "InputProcess"
        self.debug_enabled = config.config["globals"].get("debug", False)

        # Queue objects
        self.standard_queue: CollectorMultiprocessingQueue = standard_queue
        self.lookup_queue: CollectorMultiprocessingQueue = lookup_queue
        self.internal_queue: CollectorMultiprocessingQueue = internal_queue

        # Object to be used for waking up the parent process of its wait status
        self.__controller_thread_wait_object: multiprocessing.Event = controller_thread_wait_object

        self.input_threads: dict = {}

        # Extract the configuration
        self.inputs_configuration = config.config.get("inputs")
        if self.inputs_configuration is None:
            raise InputControllerException(
                1,
                "Required \"inputs\" entry not found in configuration."
            )

        # Log the configuration
        if log.isEnabledFor(logging.DEBUG):
            log_message = f"[INPUT] {__class__.__name__} -> Configuration for \"inputs\": {self.inputs_configuration}"
            log.debug(log_message)
            self.send_internal_collector_message(log_message, level="debug")

        # Loading limitations and globals

        collector_definition_globals = CollectorDefinitions.get_collector_globals()
        if not collector_definition_globals:
            raise InputControllerException(
                2,
                'Required "collector_globals" entry not found in collector definitions'
            )
        self.requests_limits = collector_definition_globals.get("requests_limits")
        self.__input_process_execution_periods_in_seconds: int = \
            collector_definition_globals["input_process_execution_periods_in_seconds"]

        self.globals_configuration: dict = config.config.get("globals")
        if self.globals_configuration is None:
            raise CollectorConfigurationException(
                11,
                "Required \"globals\" entry not found in configuration."
            )

        # Communication channel between this process (input) and the parent one (collector)
        self.__controller_commands_connection: Connection = controller_commands_connection

        # The following variables must be shareable between multiprocessing objects
        self.__controller_process_wait_object: multiprocessing.Event = multiprocessing.Event()
        self.__running_flag: multiprocessing.Value = multiprocessing.Value(ctypes.c_bool, True)

        # Definitions incompatible with multiprocessing jump will be loaded into the execution method.
        self.initial_running_flag_state = None

    def wake_up(self) -> None:
        """ Wake up the run method when it is waiting for an external input (while loop) """

        if self.__controller_process_wait_object.is_set() is False:
            self.__controller_process_wait_object.set()

    def run(self):
        """Once run() method is called, we can create objects in the new multiprocessing process"""

        try:
            self.__activate_logging()

            log.info(f"Process Started")

            self.__self_definitions()
            # self.__check_hyper_value_status()
            self.__start_threads()
            self.__watch_service()

            # self.__wait_until_all_threads_are_stopped()

            self.__send_notification_to_main_process(
                CollectorNotification.create_input_process_is_stopped_notification(
                    "Due to order received"
                )
            )

        except KeyboardInterrupt as ex:
            print(f'{self.name} - CONTROL+C received')

        alive_thread_names = []
        for t in threading.enumerate():
            if t.is_alive() and t.name != "MainThread":
                alive_thread_names.append(t.name)

        # Since this point no messages will be allowed to be sent to the internal queue
        self.internal_queue.block_input_queue()

        if alive_thread_names:
            log.info(f'Finalizing process, there are still some threads alive: {json.dumps(alive_thread_names)}')
        else:
            log.info("Finalizing process")

    def __wait_until_all_threads_are_stopped(self) -> None:
        """

        :return:
        """

        still_threads_alive = True
        while still_threads_alive:
            alive_threads = []
            for t in threading.enumerate():
                if t.is_alive() and t.name != "MainThread":
                    alive_threads.append(t.name)

            if alive_threads:
                log.warning(f'There are some alive_threads: {json.dumps(alive_threads)}')
                called: bool = self.__controller_process_wait_object.wait(timeout=5)
                if called is True:
                    self.__controller_process_wait_object.clear()
            else:
                still_threads_alive = False

    def start(self) -> None:
        """

        :return:
        """

        log.info(
            f"{self.name} - Starting thread (executing_period={self.__input_process_execution_periods_in_seconds}s)"
        )

        super().start()

    # def stop(self) -> None:
    #     """
    #
    #     :return:
    #     """
    #
    #     self.__stop_controller_thread.value = False
    #     self.__controller_thread_wait_object.set()
    #
    #     self.__running_flag.value = False
    #
    #     if self.__controller_process_wait_object.is_set() is False:
    #         self.__controller_process_wait_object.set()
    #
    #     log.info(
    #         f'{__class__.__name__}::{inspect.stack()[0][3]} -> '
    #         f'Stopped'
    #     )

    def __create_input_threads(self) -> bool:
        """Creates the threads in the tree.

        :return: True if there are available threads. False if not.
        """

        inputs_available = 0

        # Iterate inputs_configuration
        for input_name, input_config in self.inputs_configuration.items():
            if input_config:
                inputs_available = self.__create_threads(input_config, input_name, inputs_available)
            else:
                log.error('The "input" configuration entry has not the right structure')

        # Check if there are available threads
        if inputs_available > 0:
            return True

        return False

    def __create_threads(self, input_config: dict, input_name: str, inputs_available: int) -> int:
        """
        Creates a new thread with the given configuration in the input threads tree
        @param input_config: Input configuration
        @param input_name: Input name
        @param inputs_available: Number of the actual
        @returns: Number of available input threads
        """

        if "enabled" in input_config and input_config["enabled"] is True:
            input_definition = CollectorDefinitions.get_input_definitions(input_name)
            if input_definition:
                inputs_available = self.__create_thread(
                    input_config, input_definition, input_name, inputs_available
                )
        else:

            log.error(f'It exists an "input" entry for "{input_name}" but is not enabled or is missing')

        return inputs_available

    def __create_thread(self, input_config: dict, input_definition: dict, input_name: str, inputs_available: int):
        """Creates a new thread with the given definition.

        :param input_config: Input config dict
        :param input_definition: Input definition
        :param input_name: Input name
        :param inputs_available: Available threads counter
        :return:
        """
        # Get the input_id for this thread
        input_id: str = \
            str(input_config["id"]) if "id" in input_config and len(str(input_config["id"])) > 0 else None
        input_config["id"] = input_id if input_id else None

        # Create thread
        try:
            input_thread = InputThread(
                self,
                input_id,
                input_name,
                input_config,
                input_definition,
                self.standard_queue,
                self.lookup_queue,
                self.internal_queue,
                self.persistence_manager
            )

            # Log trace
            if log.isEnabledFor(logging.DEBUG):
                log_message = "{} -> {} - Instance created".format(
                    f'{__class__.__name__}::{inspect.stack()[0][3]}',
                    input_thread.getName()
                )
                log.debug(log_message)
                self.send_internal_collector_message(log_message, level="debug")

            # Store new thread
            self.input_threads[input_thread.getName()] = input_thread
            inputs_available += 1

        except ObjectPersistenceException as ex:
            if log.isEnabledFor(logging.ERROR):
                log.error(ex)
                self.send_internal_collector_message(str(ex), level="error")

        return inputs_available

    def __activate_logging(self):
        """This method config the logging for this multiprocessing process"""
        global log_level

        if self.debug_enabled is True:
            log_level = logging.DEBUG

        logging.basicConfig(
            level=log_level,
            format='%(asctime)s.%(msecs)03d %(levelname)7s %(processName)s::%(threadName)s -> %(message)s',
            datefmt='%Y-%m-%dT%H:%M:%S'
        )

    def __self_definitions(self) -> None:
        """This method populate the self definitions with pickle-incompatible procedure

        :return:
        """

        log.debug(
            f'{__class__.__name__}::{inspect.stack()[0][3]} -> Instantiating pickle-incompatible properties'
        )

        self.persistence_manager: PersistenceManager = self._create_persistence_manager()
        self.initial_running_flag_state: bool = True if self.__create_input_threads() else False

    def _create_persistence_manager(self) -> PersistenceManager:
        """

        :return:
        """
        collector_id: Optional[str, int] = self.globals_configuration.get("id")
        if collector_id is None:
            raise CollectorConfigurationException(13, "Required \"globals.id\" entry not found in configuration.")

        persistence_config: dict = self.globals_configuration.get("persistence")
        if persistence_config is None:
            raise CollectorConfigurationException(
                12, "Required \"globals.persistence\" entry not found in configuration."
            )

        persistence_manager = PersistenceManager(str(collector_id), persistence_config)

        log.debug(
            f"{__class__.__name__}::{inspect.stack()[0][3]} - Created persistence manager: {persistence_manager}"
        )

        return persistence_manager

    def __start_threads(self) -> None:
        """This method starts all threads instances"""
        if len(self.input_threads) > 0:
            for input_thread in self.input_threads.values():
                input_thread.start()

    def __watch_service(self):
        """This method watches over related thread instances status.
        While the running_flag is True,
        """

        while self.__running_flag.value:

            # Proceed to execute a memory garbage collection
            self.__cleaning_memory()

            # f"{__class__.__name__}::{inspect.stack()[0][3]} -> "

            log.debug(f'Entering in wait status')
            called: bool = \
                self.__controller_process_wait_object.wait(
                    timeout=self.__input_process_execution_periods_in_seconds
                )
            if called is True:
                self.__controller_process_wait_object.clear()

            log.debug(f'Waking up from wait status')

            self.check_pending_orders()

            self._check_inputs_status()
            if self.__no_running_threads():
                self.__running_flag.value = False

        if self.initial_running_flag_state is True:
            log.info("Stopping process")
        else:
            log.info("Nothing to do, stopping process")

    def check_pending_orders(self) -> None:
        """

        :return:
        """

        if self.__controller_commands_connection.poll():
            collector_order: CollectorOrder = self.__controller_commands_connection.recv()
            log.debug(f'Received order: {collector_order}')
            if collector_order == CollectorOrder.PAUSE_ALL_INPUT_OBJECTS:

                log.info(f'All input threads will be put on pause status')

                if len(self.input_threads) > 0:
                    for input_thread in self.input_threads.values():
                        input_thread: InputThread
                        input_thread.pause()

                log.info(f'All input threads have been paused')

                log.debug(f'A notification will be sent to the InputControllerThread')

                self.__send_notification_to_main_process(
                    CollectorNotification.create_all_inputs_are_paused_notification(
                        f'Done due to order: {collector_order}'
                    )
                )

            elif collector_order == CollectorOrder.STOP_INPUT_CONTROLLER:
                if len(self.input_threads) > 0:
                    for input_thread in self.input_threads.values():
                        if isinstance(input_thread, InputThread):
                            input_thread.stop()

                log.info(f'All input threads have been stopped')

                log.debug(f'A notification will be sent to the InputControllerThread')

                self.__running_flag.value = False

            else:
                log.error(f'Received an unknown order: {collector_order}')

    def __cleaning_memory(self) -> None:
        """They will be cleaned all the "dead" objects

        :return:
        """

        current_process = psutil.Process(os.getpid())

        memory_usage_mb_before_rss = current_process.memory_info().rss
        memory_usage_mb_before_vms = current_process.memory_info().vms
        memory_usage_info_before = psutil.virtual_memory()
        gc.collect()
        memory_usage_mb_after_rss = current_process.memory_info().rss
        memory_usage_mb_after_vms = current_process.memory_info().vms
        memory_usage_info_after = psutil.virtual_memory()

        if log.isEnabledFor(logging.INFO):
            log_message = \
                f'[GC] ' \
                f'global: ' \
                f'{memory_usage_info_before.percent}% -> ' \
                f'{memory_usage_info_after.percent}%, ' \
                f'process: RSS({CollectorUtils.human_readable_size(memory_usage_mb_before_rss)} -> ' \
                f'{CollectorUtils.human_readable_size(memory_usage_mb_after_rss)}), ' \
                f'VMS({CollectorUtils.human_readable_size(memory_usage_mb_before_vms)} -> ' \
                f'{CollectorUtils.human_readable_size(memory_usage_mb_after_vms)})'

            log.info(log_message)
            self.send_internal_collector_message(log_message, level="info")

        if log.isEnabledFor(logging.DEBUG):
            log_message = \
                f'[GC] ' \
                f'memory_before(' \
                f'total: {CollectorUtils.human_readable_size(memory_usage_info_before.total)}, ' \
                f'used: {CollectorUtils.human_readable_size(memory_usage_info_before.used)}' \
                f'), ' \
                f'memory_after(' \
                f'total: {CollectorUtils.human_readable_size(memory_usage_info_after.total)}, ' \
                f'used: {CollectorUtils.human_readable_size(memory_usage_info_after.used)}' \
                f')'
            log.debug(log_message)
            self.send_internal_collector_message(log_message, level="debug")

    def _check_inputs_status(self):
        """ Detects dead threads and mark them as removed """
        input_threads_to_remove: list = []

        # Detect the dead threads
        for input_thread_name, input_thread in self.input_threads.items():
            if isinstance(input_thread, str) is False and input_thread.is_alive() is False:
                input_threads_to_remove.append(input_thread_name)

        # Execute only if there are threads to remove
        if len(input_threads_to_remove) > 0:
            for input_thread_name in input_threads_to_remove:
                # Mark the thread as removed
                internal_input_thread_name = self.input_threads[input_thread_name].getName()
                self.input_threads[input_thread_name] = "REMOVED"

                log.info(f'Removed finalized thread: {internal_input_thread_name}')

    def __no_running_threads(self) -> bool:
        """Checks if all the input_threads are successfully removed.

        :return: True if yes, False if not.
        """
        number_of_setup_input_threads = len(self.input_threads)
        number_of_removed_threads = 0

        # Count the removed threads
        for input_thread in self.input_threads.values():
            if input_thread == "REMOVED":
                number_of_removed_threads += 1

        # Execute the checking
        if number_of_setup_input_threads != number_of_removed_threads:

            # Log trace
            if log.isEnabledFor(logging.DEBUG):
                log_message = "Still running {} of {} input threads: {}".format(
                    number_of_setup_input_threads - number_of_removed_threads,
                    number_of_setup_input_threads,
                    [*self.input_threads]
                )
                log.debug(log_message)
                self.send_internal_collector_message(log_message, level="debug")

            return False

        return True

    def __send_notification_to_main_process(self, notification: CollectorNotification):
        """

        :return:
        """
        self.__controller_commands_connection.send(notification)
        if self.__controller_thread_wait_object.is_set() is False:
            self.__controller_thread_wait_object.set()

    def send_internal_collector_message(self,
                                        message_content: Union[str, dict],
                                        level: str = None,
                                        shared_domain: bool = False):
        """This function add a new message to the internal collector.

        :param message_content: Message to be sent to Devo.
        :param level: Severity of the message.
        :param shared_domain: True if the message has <shared_domain> property to be
        watched by IF Devo Team for monitoring purposes.
        """

        # CollectorUtils.send_internal_collector_message(
        #     self.internal_queue,
        #     message_content,
        #     level=level,
        #     shared_domain=shared_domain
        # )
        pass
