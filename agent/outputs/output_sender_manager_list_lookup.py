"""
Output lookup senders receive the lookups from consumers and sends them to the available senders managers
"""

import copy
import json
import logging
from typing import Union, Optional

from agent.commons.non_active_reasons import NonActiveReasons
from agent.outputs import outputs
from agent.outputs.senders.abstracts.sender_manager_abstract import SenderManagerAbstract
from agent.queues.content.collector_queue_item import CollectorQueueItem
from agent.queues.one_to_many_communication_queue import OneToManyCommunicationQueue

log = logging.getLogger(__name__)


class OutputSenderManagerListLookup(object):
    """
    Class for managing the output senders threads for lookups
    """

    LOOKUP_COMPATIBLE_OUTPUTS = [
        'devo_platform',
        'console',
        'relay'
    ]

    def __init__(self,
                 output_controller_communication_channel: OneToManyCommunicationQueue = None,
                 output_controller_instance=None,
                 group_name: str = None,
                 outputs_configuration: Union[dict, list] = None,
                 generate_collector_details: bool = None):
        """Builder

        :param output_controller_communication_channel
        :param output_controller_instance
        :param group_name: Group assigned to the OutputSenders
        :param outputs_configuration: Output configuration
        :param generate_collector_details: Generate details about the collector execution
        :return:
        """
        # Load properties from the signature
        self.output_controller_communication_channel: OneToManyCommunicationQueue = \
            output_controller_communication_channel
        self.output_controller_instance = output_controller_instance

        self.group_name: str = group_name
        self.outputs_configuration = self.__get_lookup_compatible_outputs(outputs_configuration)
        self.__generate_collector_details: bool = generate_collector_details

        # Create required empty properties
        self.output_threads: dict[str, SenderManagerAbstract] = {}
        self.last_thread_used: int = 999
        self.status_of_lookup_service = False

        self.__create_output_threads()

    def __get_lookup_compatible_outputs(self, outputs_configuration: dict) -> dict:
        """
        Method that filters the lookup non-compatible outputs from the outputs_configuration
        @param outputs_configuration: outputs values from settings in dict format.
        @return: A new dict only with the lookup-compatible outputs.
        """
        valid_outputs: dict = {}

        for out_id, out_conf in outputs_configuration.items():
            if out_conf.get('type', 'Unknown') in self.LOOKUP_COMPATIBLE_OUTPUTS:
                valid_outputs[out_id]: dict = out_conf

        return valid_outputs

    def add_output_object(self, queue_item: CollectorQueueItem) -> dict:
        """ Add a copy of a new queue item to the outputs sender queues """
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
        @return: List of thread names
        """
        output_job: list = []

        for output_thread_name, output_thread in self.output_threads.items():
            output_job.append(output_thread_name)

        return output_job

    def __create_output_threads(self):
        """ Creates all output threads from the outputs_configuration"""

        if len(self.outputs_configuration) == 0:
            msg = (f"[OUTPUT] {type(self).__name__} -> "
                   f"Lookup Service is UNAVAILABLE due to no compatible outputs have been found.")
            log.warning(msg)
        else:
            try:
                if isinstance(self.outputs_configuration, dict):
                    self.__create_output_thread_using_dict()
                elif isinstance(self.outputs_configuration, list):
                    self.__create_output_thread_using_list()

            except Exception as ex:
                log.error(f"[OUTPUT] {type(self).__name__} -> Error when creating sending object [CODE:5001]: {ex}")
                raise

        # Verify if there are valid senders running
        if len(self.output_threads) > 0:
            self.status_of_lookup_service = True
        else:
            self.status_of_lookup_service = False

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
                        # Lookup output should create only one concurrent_connection
                        config_modified: Optional[dict] = self.__normalize_output_config(output_config)

                        output_instance = output_class(
                            output_controller_communication_channel=self.output_controller_communication_channel,
                            output_controller_instance=self.output_controller_instance,
                            group_name=self.group_name,
                            content_type='lookup',
                            instance_name=output_name,
                            configuration=config_modified if config_modified is not None else output_config,
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

    @staticmethod
    def __normalize_output_config(output_config: Union[list, dict]) -> Optional[dict]:
        """
        Method that verifies if the output_config should be modified before sending it to senders builder.
        @param output_config: a list or dict instance with the output configuration.
        @return: if the output_config should be modified, returns a dict instance with the modified configuration.
            if not, returns a None object.
        """
        modified_output_config = output_config.copy()
        modified = False

        # Setting: concurrent_connections
        if modified_output_config['config']['concurrent_connections'] != 1:
            msg = (f"[OUTPUT] {__class__.__name__} -> <concurrent_connections> setting "
                   f"has been modified from {output_config['config']['concurrent_connections']} to 1 "
                   f"due to the maximum concurrent_connections for Lookups should be 1")
            log.warning(msg)
            modified_output_config['config']['concurrent_connections']: int = 1
            modified = True

        # Setting activate_final_queue
        if modified_output_config['config']['activate_final_queue'] is True:
            msg = (f"[OUTPUT] {__class__.__name__} -> <activate_final_queue> setting has been modified from True to "
                   f"False due to this configuration increases the Lookup sender performance.")
            log.warning(msg)
            modified_output_config['config']['activate_final_queue'] = False
            modified = True

        # Setting threshold_for_using_gzip_in_transport_layer
        if modified_output_config['config']['threshold_for_using_gzip_in_transport_layer'] != 1.1:
            msg = f"[OUTPUT] {__class__.__name__} -> <threshold_for_using_gzip_in_transport_layer> setting has been " \
                  f"modified from {modified_output_config['config']['threshold_for_using_gzip_in_transport_layer']} " \
                  f"to 1.1 due to this configuration increases the Lookup sender performance."
            log.warning(msg)
            modified_output_config['config']['threshold_for_using_gzip_in_transport_layer']: float = 1.1
            modified = True

        # If no modifications has been applied to modified_output_config, assign None.
        if modified is False:
            modified_output_config = None

        return modified_output_config

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

    def start_senders(self) -> None:
        """ Starts all senders threads """
        for output_thread_name, output_thread in self.output_threads.items():
            output_thread.start()

    def __str__(self) -> str:
        """String representation of the class.

        :return:
        """

        sml_dict: dict = {
            "senders": str(self.output_threads)
        }
        return json.dumps(sml_dict)

    def stop_senders(self) -> None:
        """Stops all senders threads

        :return:
        """

        log.debug(f'Stopping sender managers: {self.group_name}')

        for output_thread in self.output_threads.values():
            output_thread.stop()

        log.debug(f'All sender managers from "{self.group_name}" have been stopped')

    def pause_senders(self) -> None:
        """ Pauses all senders threads """

        log.debug(f'Pausing sender manager list: {self.group_name}')

        for output_thread_name, output_thread in self.output_threads.items():
            if output_thread.is_paused():
                log.info(
                    f'Thread "{output_thread.name}" is already paused, '
                    f'reason: "{output_thread.non_active_reason}"'
                )
            else:
                output_thread.pause()

        log.debug(f'All sender managers from "{self.group_name}" have been paused')

    def service_status(self) -> bool:
        """ Returns the status of the lookup service """
        return self.status_of_lookup_service

    def flush_to_emergency_persistence_system(self) -> None:
        """

        :return:
        """

        log.debug(f'Flushing internal queues from {self.group_name} to emergency persistence system')

        for output_thread_name, output_thread in self.output_threads.items():
            if output_thread.has_been_flushed():
                log.info(
                    f'Thread "{output_thread_name}" has been already flushed'
                )

            else:
                output_thread.flush_to_emergency_persistence_system()

        log.debug(f'All sender managers from {self.group_name} have been flushed')

    def get_most_relevant_non_active_reason(self) -> str:
        """

        :return:
        """

        most_relevant_non_active_reason: Optional[str] = None
        for output_thread_name, output_thread in self.output_threads.items():
            non_active_reason = output_thread.non_active_reason
            if non_active_reason == NonActiveReasons.FINAL_SENDER_IS_NOT_WORKING:
                most_relevant_non_active_reason = non_active_reason
                break

        return most_relevant_non_active_reason
