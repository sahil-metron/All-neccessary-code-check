import logging
from typing import Any

from agent.outputs.senders.abstracts.sender_manager_abstract import \
    SenderManagerAbstract, ManagerMonitorType, SendersType, SenderType
from agent.outputs.senders.syslog_sender import SyslogSender
from agent.outputs.senders.syslog_sender_manager_monitor import SyslogSenderManagerMonitor
from agent.outputs.senders.syslog_senders import SyslogSenders
from agent.queues.sender_manager_queue import SenderManagerQueue

log = logging.getLogger(__name__)


class SyslogSenderManager(SenderManagerAbstract):
    """ Sender Manager Class that sends the messages to a Syslog server """

    def __init__(self, **kwargs: Any):
        """Builder.

        :param kwargs:  -> group_name (str): Group name
                        -> instance_name (str): Name of the instance
                        -> configuration (dict): Dictionary with the configuration
                        -> generate_collector_details (bool) Defines if the collector details must be generated
        """
        super().__init__(**kwargs)

        self.internal_name: str = 'syslog_sender_manager'

    def _server_configuration(self, server_configuration: dict):
        """Abstract method where the custom server configuration settings are extracted and stored.

        :param server_configuration: Dict from where the configuration settings are extracted
        :return:
        """

        # Connection settings
        self._address: str = server_configuration.get("address")
        self._port: int = int(server_configuration.get("port"))
        self._transport_layer_type: str = server_configuration.get("type")
        self._concurrent_connections: int = server_configuration["concurrent_connections"]

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

    def _senders_constructor(self) -> SendersType:
        """
        Abstract where the senders' object is built.
        @return: An instance of ConsoleSenders, DevoSenders or SyslogSenders.
        """
        return SyslogSenders(group_name=self.group_name)

    def _senders_monitor_constructor(self) -> ManagerMonitorType:
        """
        Abstract method where the senders' monitor object is built.
        @return: An instance of ConsoleSenderManagerMonitor, DevoSenderManagerMonitor or SyslogSenderManagerMonitor.
        """
        return SyslogSenderManagerMonitor(
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
        :return: An instance of SyslogSender.
        :rtype: SenderType
        :example:

            return ConsoleSender(
                self.group_name,
                self.instance_name,
                self.destination,
                final_sender_queue,
                self._generate_collector_details
            )
        """
        return SyslogSender(
            sender_manager_instance=sender_manager_instance,
            group_name=self.group_name,
            instance_name=sender_name,
            address=self._address,
            port=self._port,
            connecting_retries=self._connecting_retries,
            connecting_max_wait_in_seconds=self._connecting_max_wait_in_seconds,
            connecting_initial_wait_in_seconds=self._connecting_initial_wait_in_seconds,
            sending_timeout=self._sending_timeout,
            sending_retries=self._sending_retries,
            sending_max_wait_in_seconds=self._sending_max_wait_in_seconds,
            sending_initial_wait_in_seconds=self._sending_initial_wait_in_seconds,
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

        instance_name: str = f'syslog_sender_{sender_id}'
        return instance_name

    @staticmethod
    def process():
        """SonarLint S1144"""
        return SyslogSenderManager._sender_constructor

    @staticmethod
    def process():
        """SonarLint S1144"""
        return SyslogSenderManager._build_sender_instance_name

    @staticmethod
    def process():
        """SonarLint S1144"""
        return SyslogSenderManager._senders_monitor_constructor

    @staticmethod
    def process():
        """SonarLint S1144"""
        return SyslogSenderManager._server_configuration

    @staticmethod
    def process():
        """SonarLint S1144"""
        return SyslogSenderManager._senders_constructor
