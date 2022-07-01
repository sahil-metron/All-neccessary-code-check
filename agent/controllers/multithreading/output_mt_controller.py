import logging
import threading
from threading import Thread
from typing import Union

from agent.commons.collector_utils import CollectorUtils
from agent.configuration.configuration import CollectorConfiguration
from agent.controllers.exceptions.exceptions import OutputControllerException
from agent.outputs.output_consumer_lookup import OutputLookupConsumer
from agent.outputs.output_sender_manager_list_lookup import OutputSenderManagerListLookup
from agent.outputs.output_consumer_standard import OutputStandardConsumer
from agent.outputs.output_sender_manager_list_standard import OutputSenderManagerListStandard
from agent.queues.collector_multithreading_queue import CollectorMultithreadingQueue

# Globals
log = logging.getLogger(__name__)


class OutputMultithreadingController(Thread):
    """ Output controller class for multithreading architecture """

    def __init__(self,
                 config: CollectorConfiguration,
                 standard_queue: CollectorMultithreadingQueue,
                 lookup_queue: CollectorMultithreadingQueue,
                 internal_queue: CollectorMultithreadingQueue,
                 main_wait_object: threading.Event,
                 command_flags: dict = None):
        """Builder in charge to prepare the class properties to be used by the run method.

        :param config: an instance of CollectorConfiguration.
        :param standard_queue: Internal queue for standard messages.
        :param lookup_queue: Internal queue for lookup messages.
        :param internal_queue: Internal queue for internal messages.
        :param main_wait_object: multiprocessing.Event() from parent process
        """
        super().__init__()

        # Get the class and global properties
        self.name = self.__class__.__name__ + f"({CollectorUtils.collector_name})"
        self.standard_queue: CollectorMultithreadingQueue = standard_queue
        self.lookup_queue: CollectorMultithreadingQueue = lookup_queue
        self.internal_queue: CollectorMultithreadingQueue = internal_queue

        # Get the injected objects
        self.output_controller_wait_object = threading.Event()
        self.main_wait_object = main_wait_object

        # Extract the configuration
        self.balanced_output: bool = config.is_balanced_output()
        self.max_consumers: int = config.get_max_consumers()
        self.generate_collector_details: bool = config.generate_collector_details()

        self.outputs_configuration: Union[dict, list] = config.config.get("outputs")
        if self.outputs_configuration is None:
            raise OutputControllerException(
                0,
                "Required \"outputs\" entry not found in configuration."
            )
        log.debug(f"[OUTPUT] {self.getName()} -> Configuration for \"outputs\": {self.outputs_configuration}")

        # Config the Standard senders and consumer
        self.output_standard_senders: OutputSenderManagerListStandard = OutputSenderManagerListStandard(
            output_controller_instance=self,
            group_name="standard_senders",
            outputs_configuration=self.outputs_configuration,
            balanced_output=self.balanced_output,
            generate_collector_details=self.generate_collector_details
        )
        self.output_standard_consumer: OutputStandardConsumer = OutputStandardConsumer(
            group_name="standard_senders",
            consumer_identification=0,
            standard_queue=self.standard_queue,
            standard_senders=self.output_standard_senders,
            generate_collector_details=self.generate_collector_details
        )

        # Config the Lookups senders and consumer
        self.output_lookup_senders: OutputSenderManagerListLookup = OutputSenderManagerListLookup(
            output_controller_instance=self,
            group_name="lookup_senders",
            outputs_configuration=self.outputs_configuration,
            generate_collector_details=self.generate_collector_details
        )
        self.output_lookup_consumer: OutputLookupConsumer = OutputLookupConsumer(
            group_name="lookup_senders",
            consumer_identification=0,
            lookup_queue=self.lookup_queue,
            lookup_senders=self.output_lookup_senders,
            generate_collector_details=self.generate_collector_details
        )

        # Ready flag!
        self.running_flag: bool = True

    def run(self) -> None:
        """ Thread execution starts here after start method is called """

        # running_flag let us to know if other service need that this thread to stop.
        while self.running_flag:
            self.output_controller_wait_object.wait(timeout=300)
            log.debug(f"[{self.output_standard_consumer.name}] Data -> {self.output_standard_consumer}")
            log.debug(f"[{self.output_lookup_consumer.name}] Data -> {self.output_lookup_consumer}")

        # Stop order has been detected
        log.info(f"Finalizing thread")
        log.debug(self.output_standard_consumer)

        # Kill threads tree for standard
        self.output_standard_consumer.stop()
        self.output_standard_senders.stop_senders()

        # Kill threads tree for lookup
        self.output_lookup_consumer.stop()
        self.output_lookup_senders.stop_senders()

        self.main_wait_object.set()

    def start(self) -> None:
        """Start the threading tree.

        :return:
        """
        log.info(f"[OUTPUT] {self.getName()} -> Starting thread")

        # Standard Senders
        self.output_standard_senders.start_senders()
        self.output_standard_consumer.start()

        # Lookup Senders
        self.output_lookup_senders.start_senders()
        self.output_lookup_consumer.start()

        # Send consumer(s) status to log
        log.debug(f"output_standard_consumer: {self.output_standard_consumer}")
        log.debug(f"output_lookup_consumer: {self.output_standard_consumer}")

        super().start()

    def wake_up(self) -> None:
        """

        :return:
        """
        self.output_controller_wait_object.set()

    def stop(self) -> None:
        """ Stop the all threads tree """
        self.running_flag = False
        self.wake_up()

    def activate_flush_to_persistence_system_mode(self) -> None:
        """

        :return:
        """
        # First, avoid more message to arrive to internal queues
        print("Blocking queues for avoiding new messages")
        self.lookup_queue.block_input_queue()
        self.standard_queue.block_input_queue()

        print("Enabling flushing mode")
        self.output_lookup_consumer.enable_flush_to_persistence_system()
        self.output_standard_consumer.enable_flush_to_persistence_system()

        print("Killing the output controller")
        self.stop()
