import copy
import logging
from typing import Union

from agent.outputs import outputs
from agent.queues.content.collector_queue_item import CollectorQueueItem

log = logging.getLogger(__name__)


class OutputSenders(object):
    """ Class for managing the output senders threads """

    def __init__(self,
                 group_name: str,
                 outputs_configuration: Union[dict, list],
                 balanced_output: bool,
                 generate_collector_details: bool):
        """
        Builder
        @param group_name: Group assigned to the OutputSenders
        @param outputs_configuration: Output configuration
        @param balanced_output: Defines if the output senders must work as a load balancer.
        @param generate_collector_details: Generate details about the collector execution
        """
        # Load properties from the signature
        self.group_name: str = group_name
        self.outputs_configuration = outputs_configuration
        self.balanced_output: bool = balanced_output
        self.__generate_collector_details: bool = generate_collector_details

        # Create required empty properties
        self.output_threads: dict = {}
        self.last_thread_used: int = 999

        self.__create_output_threads()

    def add_output_object(self, queue_item: CollectorQueueItem) -> dict:
        """ Add a copy of a new queue item to the the outputs sender queues """
        statuses = {}
        output_job: [str] = self.__create_output_job()

        for output_thread_name, output_thread in self.output_threads.items():
            if output_thread_name in output_job:
                internal_queue_size = output_thread.add_output_object(copy.deepcopy(queue_item))
                statuses[output_thread.internal_name] = internal_queue_size

        return statuses

    def __create_output_job(self) -> [str]:
        """
        Creates a list of senders to whom the message must be sent.
        This method takes into consideration the balancer if defined.
        @return: List of thread names
        """
        output_job: list = []

        # Choose the index of the next sender or choose all
        if self.balanced_output:
            self.last_thread_used += 1
            if self.last_thread_used > len(self.output_threads):
                self.last_thread_used = 0
        else:
            self.last_thread_used = None

        # Select the winner(s)
        thread_id = 0
        for output_thread_name, output_thread in self.output_threads.items():
            if self.last_thread_used is None or self.last_thread_used == thread_id:
                output_job.append(output_thread_name)
            thread_id += 1

        return output_job

    def __create_output_threads(self):
        """ Creates all output threads from the outputs_configuration"""
        try:
            if isinstance(self.outputs_configuration, dict):
                self.__create_output_thread_using_dict()
            elif isinstance(self.outputs_configuration, list):
                self.__create_output_thread_using_list()
        except Exception as ex:
            log.error("Error when creating sending object [CODE:5001]: {}".format(ex))
            raise

    def __create_output_thread_using_dict(self):
        """ Creates the new output threads using a dict configuration structure """
        for output_name, output_config in self.outputs_configuration.items():
            if output_config:
                try:
                    output_class = outputs[output_config["type"]]
                except KeyError:
                    log.error(f"[OUTPUT] {type(self).__name__} -> {output_name} contains an unknown output_type")
                else:
                    if output_class:
                        output_instance = output_class(
                            group_name=self.group_name,
                            instance_name=output_name,
                            configuration=output_config,
                            generate_collector_details=self.__generate_collector_details
                        )
                        self.output_threads[output_name] = output_instance
                        log.debug(
                            f"[OUTPUT] {type(self).__name__} -> {output_instance.getName()}({output_name}) - "
                            f"Instance created"
                        )
                    else:
                        log.error(f"[OUTPUT] {type(self).__name__} -> {output_name} retrieved an invalid output_class")
            else:
                log.error(f"[OUTPUT] {type(self).__name__} -> {output_name} does not containing any property")

    def __create_output_thread_using_list(self):
        """ Creates the new output threads using a dict configuration structure """
        output_counter = {}
        for output_config in self.outputs_configuration:
            output_type = output_config["type"]
            if output_type not in output_counter:
                output_counter[output_type] = 0
            if output_type in output_counter:
                output_counter[output_type] += 1
            output_class = outputs.get(output_type)
            if output_class:
                output_name = f"{output_type}_{output_counter[output_type]}"
                output_instance = \
                    output_class(self.group_name, output_name, output_config, self.__generate_collector_details)
                self.output_threads[output_name] = output_instance
                log.debug(
                    f"[OUTPUT] {type(self).__name__} -> {output_instance.getName()}({output_name}) - "
                    f"Instance created"
                )
            else:
                log.error(
                    f"[OUTPUT] {type(self).__name__} - Output type \"{output_config['type']}\" dose not exists"
                )

    def start_senders(self):
        """ Starts all senders threads """
        for output_thread_name, output_thread in self.output_threads.items():
            output_thread.start()

    def __str__(self):
        """ String representation of the class """
        return f"{{ senders: {self.output_threads}}}"

    def stop_senders(self):
        """ Stops all senders threads """
        for output_thread_name, output_thread in self.output_threads.items():
            output_thread.stop()
