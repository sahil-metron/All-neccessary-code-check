import logging
import os
import pathlib
from typing import Any

from agent.outputs.exceptions.exceptions import DevoSenderException
from agent.outputs.senders.abstracts.sender_manager_abstract import \
    SenderManagerAbstract, ManagerMonitorType, SendersType, SenderType
from agent.outputs.senders.devo_sender import DevoSender
from agent.outputs.senders.devo_sender_manager_monitor import DevoSenderManagerMonitor
from agent.outputs.senders.devo_senders import DevoSenders
from agent.queues.sender_manager_queue import SenderManagerQueue

log = logging.getLogger(__name__)


class DevoSenderManager(SenderManagerAbstract):
    """ Sender Manager Class that sends the messages to Devo """

    def __init__(self, **kwargs: Any):
        """Builder.

        :param kwargs:  -> group_name (str): Group name
                        -> instance_name (str): Name of the instance
                        -> configuration (dict): Dictionary with the configuration
                        -> generate_collector_details (bool) Defines if the collector details must be generated
        """
        super().__init__(**kwargs)

        self.internal_name: str = 'devo_sender_manager'

    def _server_configuration(self, server_configuration: dict):
        """Abstract method where the custom server configuration settings are extracted and stored.

        :param server_configuration: Dict from where the configuration settings are extracted
        :return:
        """

        # Connection settings
        self._address: str = server_configuration.get("address")
        if self._address is None:
            self._address = server_configuration.get("url")
        self._port: int = int(server_configuration.get("port"))
        self._transport_layer_type: str = server_configuration.get("type")
        self._concurrent_connections: int = server_configuration["concurrent_connections"]

        # Security settings
        self._set_ssl_files_paths(server_configuration)
        if self._chain_path and self._cert_path and self._key_path:
            self._transport_layer_type = "SSL"
        else:
            raise DevoSenderException(
                10,
                'Devo Sender is not allowed to run without SSL feature'
            )

        # Devo compression layer
        self._threshold_for_using_gzip_in_transport_layer: float = \
            server_configuration['threshold_for_using_gzip_in_transport_layer']
        self._compression_level: int = \
            server_configuration["compression_level"]
        self._compression_buffer_in_bytes: int = \
            server_configuration["compression_buffer_in_bytes"]

        # Connection troubleshooting
        self._connecting_retries: int = \
            server_configuration.get("connecting_retries", 5)
        self._connecting_max_wait_in_seconds: int = \
            server_configuration.get('connecting_max_wait_in_seconds', 60)
        self._connecting_initial_wait_in_seconds: int = \
            server_configuration.get('connecting_initial_wait_in_seconds', 2)

        # Sending properties
        self._sending_timeout: int = \
            server_configuration.get("sending_timeout", 5)
        self._sending_retries: int = \
            server_configuration.get("sending_retries", 5)
        self._sending_max_wait_in_seconds: int = \
            server_configuration.get("sending_max_wait_in_seconds", 60)
        self._sending_initial_wait_in_seconds: int = \
            server_configuration.get("sending_initial_wait_in_seconds", 2)

        self._sending_wait_between_retries_in_seconds: int = \
            server_configuration.get("sending_wait_between_retries_in_seconds", 1)

    def _set_ssl_files_paths(self, server_configuration: dict):
        """
        Method that dynamically build the paths to the required files to open the SSL connection to Devo.
        @param server_configuration: Dict with the server_configuration
        """
        # Working variables
        required_paths: [str] = ['chain', 'cert', 'key']
        count: int = 1
        self._chain_path = None
        self._cert_path = None
        self._key_path = None

        # Roam all required files
        for required_path in required_paths:
            path_to_file: str = server_configuration.get(required_path)

            if path_to_file is None:
                path_to_file = server_configuration.get(f"{required_path}_loc")

            if path_to_file:
                if os.path.isabs(path_to_file) is False:
                    path_to_file = os.path.join(os.getcwd(), "certs", path_to_file)
                config_full_file_path = pathlib.Path(path_to_file)

                if config_full_file_path.is_dir():
                    raise DevoSenderException(count, f'"{path_to_file}" is not a file')
                elif config_full_file_path.exists() is False:
                    raise DevoSenderException(count+1, f'File "{path_to_file}" does not exists')

            # Create the final class property -> example: self._chain_path/self._cert_path/self._key_path = path_to_file
            required_path = f'_{required_path}_path'
            if path_to_file:
                setattr(self, required_path, path_to_file)
            else:
                setattr(self, required_path, None)

            # Update the exception error id
            count += 2

    def _senders_constructor(self) -> SendersType:
        """
        Abstract where the senders' object is built.
        @return: An instance of ConsoleSenders, DevoSenders or SyslogSenders.
        """
        return DevoSenders(group_name=self.group_name, threshold=0.75)

    def _senders_monitor_constructor(self) -> ManagerMonitorType:
        """
        Abstract method where the senders' monitor object is built.
        @return: An instance of ConsoleSenderManagerMonitor, DevoSenderManagerMonitor or SyslogSenderManagerMonitor.
        """
        return DevoSenderManagerMonitor(
            manager_to_monitor=self,
            content_type=self.content_type,
            period_sender_stats_in_seconds=self._period_sender_stats_in_seconds)

    def _sender_constructor(self,
                            queue: SenderManagerQueue,
                            sender_name: str,
                            sender_manager_instance=None) -> SenderType:
        """Abstract method where the sender objects are built.

        :param queue: QueueType that will be injected to the sender.
        :param sender_name: Name given to the sender instance.
        :param sender_manager_instance: Sender Manager instance for waking up when errors.
        :return: An instance of DevoSender.
        :rtype: SenderType
        :Example:

            return ConsoleSender(
                self.group_name,
                self.instance_name,
                self.destination,
                final_sender_queue,
                self._generate_collector_details
            )
        """
        return DevoSender(
            sender_manager_instance=sender_manager_instance,
            group_name=self.group_name,
            instance_name=sender_name,
            address=self._address,
            port=self._port,
            transport_layer_type=self._transport_layer_type,
            chain_path=self._chain_path,
            cert_path=self._cert_path,
            key_path=self._key_path,
            connecting_retries=self._connecting_retries,
            connecting_max_wait_in_seconds=self._connecting_max_wait_in_seconds,
            connecting_initial_wait_in_seconds=self._connecting_initial_wait_in_seconds,
            sending_timeout=self._sending_timeout,
            sending_retries=self._sending_retries,
            sending_max_wait_in_seconds=self._sending_max_wait_in_seconds,
            sending_initial_wait_in_seconds=self._sending_initial_wait_in_seconds,
            threshold_for_using_gzip_in_transport_layer=self._threshold_for_using_gzip_in_transport_layer,
            compression_level=self._compression_level,
            compression_buffer_in_bytes=self._compression_buffer_in_bytes,
            content_queue=queue,
            generate_collector_details=self._generate_collector_details
        )

    def _internal_queue_percentage_usage(self) -> float:
        """
        Method that returns the usage's percentage of the internal queue.
        @return: Percentage of the queue in float format."""
        percentage = self._sender_manager_queue.sending_queue_size() / self._sender_manager_queue.maxsize
        return percentage if self._sender_manager_queue.maxsize else 0

    def _build_sender_instance_name(self, sender_id: int) -> str:
        """
        Method that returns the base-name to be used by the senders.
        @param sender_id: Sender number or id. Example: 4
        @returns: Base-name in string format. Example: 'console_sender_1'
        @sample:
        return f"super_sender_{sender_id}"
        """

        # If @staticmethod is used the signature does not comply with the abstracted method
        _ = self.internal_name

        instance_name: str = f'devo_sender_{sender_id}'
        return instance_name

    @staticmethod
    def process():
        """SonarLint S1144"""
        return DevoSenderManager._sender_constructor

    @staticmethod
    def process():
        """SonarLint S1144"""
        return DevoSenderManager._build_sender_instance_name

    @staticmethod
    def process():
        """SonarLint S1144"""
        return DevoSenderManager._senders_monitor_constructor

    @staticmethod
    def process():
        """SonarLint S1144"""
        return DevoSenderManager._server_configuration

    @staticmethod
    def process():
        """SonarLint S1144"""
        return DevoSenderManager._senders_constructor
