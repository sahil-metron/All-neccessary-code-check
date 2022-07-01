import gc
import logging
import threading
from threading import Thread
from typing import Union, Optional

import psutil

from agent.collectordefinitions.collector_definitions import CollectorDefinitions
from agent.commons.collector_utils import CollectorUtils
from agent.configuration.configuration import CollectorConfiguration
from agent.configuration.exceptions import CollectorConfigurationException
from agent.inputs.input_thread import InputThread
from agent.persistence.exceptions.exceptions import ObjectPersistenceException
from agent.persistence.persistence_manager import PersistenceManager
from agent.queues.collector_multithreading_queue import CollectorMultithreadingQueue

# Globals
log = logging.getLogger(__name__)


class InputMultithreadingController(Thread):
    """ Input controller class for multithreading architecture """

    def __init__(self,
                 config: CollectorConfiguration,
                 output_queue: CollectorMultithreadingQueue,
                 lookup_queue: CollectorMultithreadingQueue,
                 internal_queue: CollectorMultithreadingQueue,
                 main_wait_object: threading.Event,
                 command_flags: dict = None):
        """Builder in charge to prepare the class properties to be used by the run method.

        It is in charge of puller and setup instances.
        :param config: a CollectorConfiguration object.
        :param output_queue: an internal multiprocessing queue instance used for sending the standard messages.
        :param lookup_queue: an internal multiprocessing queue instance used for sending the lookup messages.
        :param main_wait_object: multiprocessing.Event() from parent process
        :return:
        """
        super().__init__()

        # Get the class and global properties
        self.name = self.__class__.__name__ + f"({CollectorUtils.collector_name})"

        # Queue objects
        self.output_queue: CollectorMultithreadingQueue = output_queue
        self.lookup_queue: CollectorMultithreadingQueue = lookup_queue
        self.internal_queue: CollectorMultithreadingQueue = internal_queue

        # Get the injected objects and create new if necessary
        self.input_controller_wait_object = threading.Event()
        self.main_wait_object = main_wait_object

        # Extract the configuration
        self.inputs_configuration: dict = config.config.get("inputs")
        if self.inputs_configuration is None:
            raise CollectorConfigurationException(10, "Required \"inputs\" entry not found in configuration.")

        # Log the configuration
        log_message = f"[INPUT] {self.getName()} -> Configuration for \"inputs\": {self.inputs_configuration}"
        log.debug(log_message)
        if log.isEnabledFor(logging.DEBUG):
            self.__send_internal_collector_message(log_message, level="debug")

        # Loading limitations and globals
        self.requests_limits = None
        self.input_controller_execution_periods_in_seconds: int = 600
        self.input_controller_globals = CollectorDefinitions.get_collector_globals()
        if self.input_controller_globals:
            self.requests_limits = self.input_controller_globals.get("requests_limits")
            self.input_controller_execution_periods_in_seconds = \
                self.input_controller_globals.get("input_controller_execution_periods_in_seconds")

        self.globals_configuration: dict = config.config.get("globals")
        if self.globals_configuration is None:
            raise CollectorConfigurationException(
                11, "Required \"globals\" entry not found in configuration."
            )

        collector_id: Optional[str, int] = self.globals_configuration.get("id")
        if collector_id is None:
            raise CollectorConfigurationException(
                13, "Required \"globals.id\" entry not found in configuration."
            )

        persistence_config: dict = self.globals_configuration.get("persistence")
        if persistence_config is None:
            raise CollectorConfigurationException(
                12,
                "Required \"globals.persistence\" entry not found in configuration."
            )

        self.persistence_manager: PersistenceManager = \
            self._create_persistence_manager(collector_id, persistence_config)

        # Instantiate input threads
        self.input_threads: dict = {}
        self.initial_running_flag_state: bool = self.__create_input_threads()
        self.running_flag: bool = True if self.initial_running_flag_state is True else False

    def _create_persistence_manager(self, collector_id: Union[str, int], persistence_config: dict):
        persistence_manager = PersistenceManager(str(collector_id), persistence_config)
        log.debug(f"[INPUT] {self.getName()} - Created persistence manager: {persistence_manager}")
        return persistence_manager

    def __create_input_threads(self) -> bool:
        """
        Creates the threads in the tree.
        :returns: True if there are available threads. False if not.
        """
        inputs_available: int = 0

        # Iterate inputs_configuration
        for input_name, input_config in self.inputs_configuration.items():
            if input_config:
                inputs_available = self.__create_threads(input_config, input_name, inputs_available)
            else:
                if log.isEnabledFor(logging.ERROR):
                    log_message = "[INPUT] The \"input\" configuration entry has not the right structure"
                    log.error(log_message)
                    self.__send_internal_collector_message(log_message, level="error")

        # Check if there are available threads
        if inputs_available > 0:
            return True

        return False

    def __create_threads(self, input_config: dict, input_name: str, inputs_available: int) -> int:
        """
        Creates a new thread with the given configuration in the input threads tree
        :param input_config: Input configuration
        :param input_name: Input name
        :param inputs_available: Number of the actual
        :returns: Number of available input threads
        """

        if "enabled" in input_config and input_config["enabled"] is True:
            input_definition = CollectorDefinitions.get_input_definitions(input_name)
            if input_definition:
                inputs_available = self.__create_thread(
                    input_config, input_definition, input_name, inputs_available
                )
        else:
            if log.isEnabledFor(logging.ERROR):
                log_message = \
                    f'[INPUT] It exists an "input" entry for "{input_name}" but is not enabled or is missing'
                log.error(log_message)
                self.__send_internal_collector_message(log_message, level="error")

        return inputs_available

    def __create_thread(self, input_config: dict, input_definition: dict, input_name: str, inputs_available: int):
        """
        Creates a new thread with the given definition.
        :param input_config: Input config dict
        :param input_definition: Input definition
        :param input_name: Input name
        :param inputs_available: Available threads counter
        """
        # Get the input_id for this thread
        input_id: str = \
            str(input_config["id"]) if "id" in input_config and len(str(input_config["id"])) > 0 else None
        input_config["id"] = input_id if input_id else None

        # Create thread
        try:
            input_thread = \
                InputThread(self,
                            input_id,
                            input_name,
                            input_config,
                            input_definition,
                            self.output_queue,
                            self.lookup_queue,
                            self.internal_queue,
                            self.persistence_manager
                            )

            # Log trace
            if log.isEnabledFor(logging.DEBUG):
                log_message = \
                    "[INPUT] {} -> {} - Instance created".format(self.getName(), input_thread.getName())
                log.debug(log_message)
                self.__send_internal_collector_message(log_message, level="debug")

            # Store new thread
            self.input_threads[input_thread.getName()] = input_thread
            inputs_available += 1

        except ObjectPersistenceException as ex:
            log.error(ex)
            if log.isEnabledFor(logging.ERROR):
                self.__send_internal_collector_message(str(ex), level="error")

        return inputs_available

    def wake_up(self) -> None:
        """ Wake up the run method when it is waiting for an external input (while loop) """
        self.input_controller_wait_object.set()

    def run(self) -> None:
        """ Thread execution starts here after start method is called """

        # running_flag let us to know if there are alive threads in the tree
        while self.running_flag:
            # Proceed to execute a memory garbage collection
            self.__cleaning_memory()

            # Wait until timeout or when an external signal has detected (wake_up method)
            called: bool = \
                self.input_controller_wait_object.wait(timeout=self.input_controller_execution_periods_in_seconds)
            if called:
                self.input_controller_wait_object.clear()
            # Log trace
            if log.isEnabledFor(logging.DEBUG):
                log_message = "Starting loop"
                log.debug(log_message)
                self.__send_internal_collector_message(log_message, level="debug")

            # Initiate the suicide if the threads are dead
            self.__check_inputs_status()
            if self.__no_running_threads():
                self.running_flag = False

        # Suicide
        if self.initial_running_flag_state is True:
            if log.isEnabledFor(logging.ERROR):
                log_message = "Finalizing thread"
                log.error(log_message)
                self.__send_internal_collector_message(log_message, level="error")
        else:
            if log.isEnabledFor(logging.INFO):
                log_message = "Nothing to do, finalizing thread"
                log.info(log_message)
                self.__send_internal_collector_message(log_message, level="info")
        self.main_wait_object.set()

    def __cleaning_memory(self) -> None:
        """They will be cleaned all the "dead" objects

        :return:
        """
        memory_usage_info_before = psutil.virtual_memory()
        gc.collect()
        memory_usage_info_after = psutil.virtual_memory()

        if log.isEnabledFor(logging.INFO):
            log_message = \
                f'Executed the memory garbage collector, ' \
                f'used_percent_changes: {memory_usage_info_before.percent}% -> {memory_usage_info_after.percent}%'
            log.info(log_message)
            self.__send_internal_collector_message(log_message, level="info")

        if log.isEnabledFor(logging.DEBUG):
            log_message = \
                f'memory_before(' \
                f'total: {CollectorUtils.human_readable_size(memory_usage_info_before.total)}, ' \
                f'used: {CollectorUtils.human_readable_size(memory_usage_info_before.used)}' \
                f'), memory_after(' \
                f'total: {CollectorUtils.human_readable_size(memory_usage_info_after.total)}, ' \
                f'used: {CollectorUtils.human_readable_size(memory_usage_info_after.used)}' \
                f')'
            log.debug(log_message)
            self.__send_internal_collector_message(log_message, level="debug")

    def start(self) -> None:
        """ Start the threading tree """
        if log.isEnabledFor(logging.INFO):
            log_message = \
                f"[INPUT] {self.getName()} - Starting thread " \
                f"(executing_period={self.input_controller_execution_periods_in_seconds}s)"
            log.info(log_message)
            self.__send_internal_collector_message(log_message, level="info")

        # Start the defined threads
        if len(self.input_threads) > 0:
            for input_thread in self.input_threads.values():
                input_thread.start()

        super().start()

    def stop(self) -> None:
        """

        :return:
        """

        if len(self.input_threads) > 0:
            for input_thread in self.input_threads.values():
                input_thread.stop()

    def __check_inputs_status(self):
        """ Detects dead threads and mark them as removed """
        input_threads_to_remove: list = []

        # Detect the dead threads
        for input_thread_name, input_thread in self.input_threads.items():
            if isinstance(input_thread, str) is False and input_thread.is_alive() is False:
                input_threads_to_remove.append(input_thread_name)

        # Execute only if there are threads to remove
        if len(input_threads_to_remove) > 0:
            for input_thread_name in input_threads_to_remove:
                # Mark thread as removed
                internal_input_thread_name = self.input_threads[input_thread_name].getName()
                self.input_threads[input_thread_name] = "REMOVED"

                # Log trace
                if log.isEnabledFor(logging.INFO):
                    log_message = f"Removed finalized thread: {internal_input_thread_name}"
                    log.info(log_message)
                    self.__send_internal_collector_message(log_message, level="info")

    def __no_running_threads(self) -> bool:
        """Checks if all the input_threads are successfully removed.

        :return: True if yes, False if not.
        """
        number_of_setup_input_threads = len(self.input_threads)
        number_of_removed_threads = 0

        # Count the removed threads
        for setup_input_threads in self.input_threads.values():
            if setup_input_threads == "REMOVED":
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
                self.__send_internal_collector_message(log_message, level="debug")

            return False

        return True

    def __send_internal_collector_message(self,
                                          message_content: Union[str, dict],
                                          level: str = None,
                                          shared_domain: bool = False):
        """
        This function add a new message to the internal collector.
        :param message_content: Message to be sent to Devo.
        :param level: Severity of the message.
        :param shared_domain: True if the message has <shared_domain> property to
            be watched by IF Devo Team for monitoring purposes.
        """
        CollectorUtils.send_internal_collector_message(
            self.internal_queue,
            message_content,
            level=level,
            shared_domain=shared_domain
        )
