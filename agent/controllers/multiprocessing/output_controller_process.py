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
from agent.controllers.exceptions.exceptions import OutputControllerException
from agent.outputs.output_consumer_internal import OutputInternalConsumer
from agent.outputs.output_consumer_lookup import OutputLookupConsumer
from agent.outputs.output_consumer_standard import OutputStandardConsumer
from agent.outputs.output_sender_manager_list_internal import OutputSenderManagerListInternal
from agent.outputs.output_sender_manager_list_lookup import OutputSenderManagerListLookup
from agent.outputs.output_sender_manager_list_standard import OutputSenderManagerListStandard
from agent.queues.collector_multiprocessing_queue import CollectorMultiprocessingQueue
from agent.queues.content.collector_notification import CollectorNotification
from agent.queues.content.collector_order import CollectorOrder
from agent.queues.content.communication_queue_notification import CommunicationQueueNotification
from agent.queues.one_to_many_communication_queue import OneToManyCommunicationQueue

log = logging.getLogger(__name__)
logging.getLogger("log").setLevel(logging.ERROR)

log_level = logging.INFO


class OutputMultiprocessingController(Process):
    """ Output controller class for multiprocessing architecture """

    def __init__(self,
                 config: CollectorConfiguration,
                 standard_queue: CollectorMultiprocessingQueue,
                 lookup_queue: CollectorMultiprocessingQueue,
                 internal_queue: CollectorMultiprocessingQueue,
                 controller_thread_wait_object: multiprocessing.Event = None,
                 controller_commands_connection: Connection = None):
        """This class creates all output services in an isolated multiprocessing process.

        It is in charge of consumers and senders instances.
        :param config: an instance of CollectorConfiguration.
        :param standard_queue: Internal queue for standard messages.
        :param lookup_queue: Internal queue for lookup messages.
        :param internal_queue: Internal queue for internal messages.
        :param controller_thread_wait_object: multiprocessing.Event() from parent process
        :param controller_commands_connection:
        """
        super().__init__()

        # Get the class and global properties
        self.name = "OutputProcess"
        self.log_debug_level = config.config["globals"].get("debug", False)

        # Queue objects
        self.standard_queue: CollectorMultiprocessingQueue = standard_queue
        self.lookup_queue: CollectorMultiprocessingQueue = lookup_queue
        self.internal_queue: CollectorMultiprocessingQueue = internal_queue

        # Object to be used for waking up the parent process of its wait status
        self.__controller_thread_wait_object: multiprocessing.Event = controller_thread_wait_object

        collector_definition_globals = CollectorDefinitions.get_collector_globals()
        if not collector_definition_globals:
            raise OutputControllerException(
                2,
                'Required "collector_globals" entry not found in collector definitions'
            )
        self.requests_limits = collector_definition_globals.get("requests_limits")
        self.__output_process_execution_periods_in_seconds: int = \
            collector_definition_globals["output_process_execution_periods_in_seconds"]

        # Extract the configuration
        self.balanced_output: bool = config.is_balanced_output()
        self.max_consumers: int = config.get_max_consumers()
        self.generate_collector_details: bool = config.generate_collector_details()

        self.outputs_configuration: Union[dict, list] = config.config.get("outputs")
        if self.outputs_configuration is None:
            raise OutputControllerException(
                1,
                'Required "outputs" entry not found in configuration'
            )

        # Log trace
        log_header = f"[OUTPUT] {__class__.__name__}::{inspect.stack()[0][3]} Configuration ->"
        log.info(f"{log_header} {self.outputs_configuration}")

        # Communication channel between this process (output) and the parent one (collector)
        self.__controller_commands_connection: Connection = controller_commands_connection

        # The following variables must be shareable between multiprocessing objects
        self.__controller_process_wait_object: multiprocessing.Event = multiprocessing.Event()
        self.__running_flag: multiprocessing.Value = multiprocessing.Value(ctypes.c_bool, True)

        # Definitions incompatible with multiprocessing jump will be loaded into the execution method.
        self.output_sender_manager_list_standard: Optional[OutputSenderManagerListStandard] = None
        self.output_standard_consumer: Optional[OutputStandardConsumer] = None
        self.output_sender_manager_list_lookup: Optional[OutputSenderManagerListLookup] = None
        self.output_lookup_consumer: Optional[OutputLookupConsumer] = None
        self.output_sender_manager_list_internal: Optional[OutputSenderManagerListInternal] = None
        self.output_internal_consumer: Optional[OutputInternalConsumer] = None

        self.process_command: Optional[str] = None
        self.processed_command: bool = False

        self.output_senders = None
        self.output_consumers = None

        self.__communication_channel: Optional[OneToManyCommunicationQueue] = None

        self.__sender_managers_paused: Optional[bool] = None
        self.__sender_managers_flushed: Optional[bool] = None
        self.__sender_managers_stopped: Optional[bool] = None

        self.__consumers_flushed: Optional[bool] = None
        self.__consumers_paused: Optional[bool] = None
        self.__consumers_stopped: Optional[bool] = None

    def run(self) -> None:
        """Once run() method is called, we can create objects in the new multiprocessing process

        :return:
        """

        try:
            self.__activate_logging()

            log.info(f"Process started")

            self.__self_definitions()
            self.__instantiate_threads()
            self.__start_threads()
            self.__watch_service()

            # self.__wait_until_all_threads_are_stopped()
            notification_to_oc_thread: CollectorNotification = \
                CollectorNotification.create_output_process_is_stopped_notification(
                    f'Due to order received'
                )
            self.__send_notification_to_main_process(notification_to_oc_thread)

        except KeyboardInterrupt as ex:
            print(f'{self.name} - CONTROL+C received')

        alive_thread_names = []
        for t in threading.enumerate():
            if t.is_alive() and t.name != "MainThread":
                alive_thread_names.append(t.name)

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
            f"{self.name} - Starting thread (executing_period={self.__output_process_execution_periods_in_seconds}s)"
        )

        super().start()

    # def stop(self) -> None:
    #     """Stop the all threads tree
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
    #         f'[OUTPUT] {__class__.__name__}::{inspect.stack()[0][3]} -> '
    #         f'Stopped'
    #     )

    def __activate_logging(self) -> None:
        """This method config the logging for this multiprocessing process"""
        global log_level

        if self.log_debug_level:
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
        # self.name = self.__class__.__name__ + f"({CollectorUtils.collector_name})"

        log_message = \
            f"[OUTPUT] {__class__.__name__}::{inspect.stack()[0][3]} -> " \
            f"Instantiating pickle-incompatible properties"
        log.debug(log_message)

        self.__communication_channel = OneToManyCommunicationQueue()

        self.__sender_managers_paused = False
        self.__sender_managers_flushed = False
        self.__sender_managers_stopped = False

        self.__consumers_paused = False
        self.__consumers_flushed = False
        self.__consumers_stopped = False

    def __instantiate_threads(self) -> None:
        """This method instantiates the threads workers

        :return:
        """

        log.debug(f"[OUTPUT] {__class__.__name__}::{inspect.stack()[0][3]} -> Instantiating threads")

        # Config the Standard senders and consumer
        self.output_sender_manager_list_standard: OutputSenderManagerListStandard = OutputSenderManagerListStandard(
            output_controller_communication_channel=self.__communication_channel,
            output_controller_instance=self,
            group_name="standard_senders",
            outputs_configuration=self.outputs_configuration,
            generate_collector_details=self.generate_collector_details
        )
        self.output_standard_consumer: OutputStandardConsumer = OutputStandardConsumer(
            output_controller_communication_channel=self.__communication_channel,
            output_controller_instance=self,
            group_name="standard_senders",
            consumer_identification=0,
            content_queue=self.standard_queue,
            sender_manager_list=self.output_sender_manager_list_standard,
            generate_collector_details=self.generate_collector_details
        )

        # Config the Lookups senders and consumer
        self.output_sender_manager_list_lookup: OutputSenderManagerListLookup = OutputSenderManagerListLookup(
            output_controller_communication_channel=self.__communication_channel,
            output_controller_instance=self,
            group_name="lookup_senders",
            outputs_configuration=self.outputs_configuration,
            generate_collector_details=self.generate_collector_details
        )
        self.output_lookup_consumer: OutputLookupConsumer = OutputLookupConsumer(
            output_controller_communication_channel=self.__communication_channel,
            output_controller_instance=self,
            group_name="lookup_senders",
            consumer_identification=0,
            content_queue=self.lookup_queue,
            sender_manager_list=self.output_sender_manager_list_lookup,
            generate_collector_details=self.generate_collector_details
        )

        # Config the Internal senders and consumer
        self.output_sender_manager_list_internal: OutputSenderManagerListInternal = OutputSenderManagerListInternal(
            output_controller_communication_channel=self.__communication_channel,
            output_controller_instance=self,
            group_name="internal_senders",
            outputs_configuration=self.outputs_configuration,
            generate_collector_details=self.generate_collector_details
        )
        self.output_internal_consumer: OutputInternalConsumer = OutputInternalConsumer(
            output_controller_communication_channel=self.__communication_channel,
            output_controller_instance=self,
            group_name="internal_senders",
            consumer_identification=0,
            content_queue=self.internal_queue,
            sender_manager_list=self.output_sender_manager_list_internal,
            generate_collector_details=self.generate_collector_details
        )

    def __start_threads(self) -> None:
        """This method starts all threads instances"""
        log.debug(f"[OUTPUT] {__class__.__name__}::{inspect.stack()[0][3]} -> Starting threads")

        # Standard Senders
        self.output_sender_manager_list_standard.start_senders()
        self.output_standard_consumer.start()

        # Lookup Senders
        self.output_sender_manager_list_lookup.start_senders()
        self.output_lookup_consumer.start()

        # Internal Senders
        self.output_sender_manager_list_internal.start_senders()
        self.output_internal_consumer.start()

        # Send consumer(s) status to log
        log.debug(f"{__class__.__name__} -> output_standard_consumer: {self.output_standard_consumer}")
        log.debug(f"{__class__.__name__} -> output_lookup_consumer: {self.output_lookup_consumer}")
        log.debug(f"{__class__.__name__} -> output_internal_consumer: {self.output_internal_consumer}")

    def __watch_service(self) -> None:
        """This method adjusts the number of consumer based on the load of the queue.
        When running_flag is False, stop the senders and consumers and die."""

        # running_flag let us know if other service need that this thread to stop.
        while self.__running_flag.value:

            # Proceed to execute a memory garbage collection
            self.__cleaning_memory()

            log.debug(f'Entering in wait status')
            called: bool = self.__controller_process_wait_object.wait(
                timeout=self.__output_process_execution_periods_in_seconds
            )
            if called:
                self.__controller_process_wait_object.clear()
            log.debug(f'Waking up from wait status')

            self.__check_pending_notifications()

            self.__check_pending_orders()

    def __check_pending_notifications(self) -> None:

        if self.__communication_channel.has_pending_notification():
            log.debug(f'There is at least one pending notification')
            while self.__communication_channel.has_pending_notification() is True:

                notification: CommunicationQueueNotification = self.__communication_channel.get_notification()
                log.debug(f'Processing notification: {notification}')

                if notification == CollectorNotification.FINAL_SENDER_IS_NOT_WORKING:
                    log.debug(
                        f'It has been received a notification about one final sender is not working, '
                        f'so it will be re-send to "MainProcess"'
                    )
                    notification_to_oc_thread: CollectorNotification = \
                        CollectorNotification.create_final_sender_is_not_working_notification(
                            notification.details
                        )
                    self.__send_notification_to_main_process(notification_to_oc_thread)

    def __check_pending_orders(self) -> None:
        """

        :return:
        """

        if self.__controller_commands_connection.poll() is True:
            collector_order: CollectorOrder = self.__controller_commands_connection.recv()
            log.debug(f'Received order: {collector_order}')

            if collector_order == CollectorOrder.PAUSE_ALL_SENDER_MANAGERS:

                log.info(f'All sender managers will be put on pause status')

                if self.__sender_managers_paused is False:
                    self.output_sender_manager_list_standard.pause_senders()
                    self.output_sender_manager_list_lookup.pause_senders()
                    self.output_sender_manager_list_internal.pause_senders()

                    self.__sender_managers_paused = True

                notification_to_oc_thread: CollectorNotification = \
                    CollectorNotification.create_all_sender_managers_are_paused_notification(
                        f'Due to order received: {collector_order}'
                    )
                self.__send_notification_to_main_process(notification_to_oc_thread)

            elif collector_order == CollectorOrder.PAUSE_ALL_CONSUMERS:

                if self.__consumers_paused is False:
                    self.output_standard_consumer.pause(wait=False)
                    self.output_lookup_consumer.pause(wait=False)
                    self.output_internal_consumer.pause(wait=False)

                    self.__consumers_paused = True

                notification_to_oc_thread: CollectorNotification = \
                    CollectorNotification.create_all_consumers_are_paused_notification(
                        f'Due to order received: {collector_order}'
                    )
                self.__send_notification_to_main_process(notification_to_oc_thread)

            elif collector_order == CollectorOrder.FLUSH_ALL_SENDER_MANAGERS:

                if self.__sender_managers_flushed is False:
                    self.output_sender_manager_list_standard.flush_to_emergency_persistence_system()
                    self.output_sender_manager_list_lookup.flush_to_emergency_persistence_system()
                    self.output_sender_manager_list_internal.flush_to_emergency_persistence_system()

                    self.__sender_managers_flushed = True

                notification_to_oc_thread: CollectorNotification = \
                    CollectorNotification.create_all_sender_managers_are_flushed_notification(
                        f'Due to order received: {collector_order}'
                    )
                self.__send_notification_to_main_process(notification_to_oc_thread)

            elif collector_order == CollectorOrder.FLUSH_ALL_CONSUMERS:

                if self.__consumers_flushed is False:
                    self.output_standard_consumer.flush_to_emergency_persistence_system(wait=False)
                    self.output_lookup_consumer.flush_to_emergency_persistence_system(wait=False)
                    self.output_internal_consumer.flush_to_emergency_persistence_system(wait=False)

                    self.__consumers_flushed = True

                notification_to_oc_thread: CollectorNotification = \
                    CollectorNotification.create_all_consumers_are_flushed_notification(
                        f'Due to order received: {collector_order}'
                    )
                self.__send_notification_to_main_process(notification_to_oc_thread)

            elif collector_order == CollectorOrder.STOP_OUTPUT_CONTROLLER:

                if self.__consumers_stopped is False:
                    self.output_standard_consumer.stop()
                    self.output_lookup_consumer.stop()
                    self.output_internal_consumer.stop()

                    self.__consumers_stopped = True

                if self.__sender_managers_stopped is False:
                    self.output_sender_manager_list_standard.stop_senders()
                    self.output_sender_manager_list_lookup.stop_senders()
                    self.output_sender_manager_list_internal.stop_senders()

                    self.__sender_managers_stopped = True

                self.__running_flag.value = False

            else:
                log.error(f'Received and unknown order: {collector_order}')

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

        log.info(
            f'[GC] '
            f'global: '
            f'{memory_usage_info_before.percent}% -> '
            f'{memory_usage_info_after.percent}%, '
            f'process: RSS({CollectorUtils.human_readable_size(memory_usage_mb_before_rss)} -> '
            f'{CollectorUtils.human_readable_size(memory_usage_mb_after_rss)}), '
            f'VMS({CollectorUtils.human_readable_size(memory_usage_mb_before_vms)} -> '
            f'{CollectorUtils.human_readable_size(memory_usage_mb_after_vms)})'
        )

        if log.isEnabledFor(logging.DEBUG):
            log_message = \
                f'memory_before(' \
                f'total: {CollectorUtils.human_readable_size(memory_usage_info_before.total)}, ' \
                f'used: {CollectorUtils.human_readable_size(memory_usage_info_before.used)}' \
                f'), ' \
                f'memory_after(' \
                f'total: {CollectorUtils.human_readable_size(memory_usage_info_after.total)}, ' \
                f'used: {CollectorUtils.human_readable_size(memory_usage_info_after.used)}' \
                f')'
            log.debug(log_message)

    def wake_up(self) -> None:
        """

        :return:
        """
        if self.__controller_process_wait_object.is_set() is False:
            self.__controller_process_wait_object.set()

    def __send_notification_to_main_process(self, notification: CollectorNotification):
        """

        :return:
        """
        self.__controller_commands_connection.send(notification)
        if self.__controller_thread_wait_object.is_set() is False:
            self.__controller_thread_wait_object.set()
