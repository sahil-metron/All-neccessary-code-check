import logging
from datetime import datetime
from typing import Optional, Any

from agent.outputs.senders.abstracts.sender_manager_abstract import \
    SenderManagerAbstract, ManagerMonitorType, SendersType, SenderType
from agent.outputs.senders.console_sender import ConsoleSender
from agent.outputs.senders.console_sender_manager_monitor import ConsoleSenderManagerMonitor
from agent.outputs.senders.console_senders import ConsoleSenders
from agent.queues.sender_manager_queue import SenderManagerQueue

log = logging.getLogger(__name__)


class ConsoleSenderManager(SenderManagerAbstract):
    """ Sender Manager Class that sends the messages to the console """

    def __init__(self, **kwargs: Optional[Any]):
        """Builder

        :param kwargs:  -> group_name (str): Group name
                        -> instance_name (str): Name of the instance
                        -> configuration (dict): Dictionary with the configuration
                        -> generate_collector_details (bool) Defines if the collector details must be generated
        """
        super().__init__(**kwargs)

        self.internal_name: str = 'console_sender_manager'

    def _server_configuration(self, server_configuration: dict):
        """Abstract method where the custom server configuration settings are extracted and stored.

        :param server_configuration: Dict from where the configuration settings are extracted
        :return:
        """

        # Connection settings
        self._destination: str = server_configuration.get("destination", 'standard')

    def _senders_constructor(self) -> SendersType:
        """
        Abstract where the senders' object is built.
        @return: An instance of ConsoleSenders, DevoSenders or SyslogSenders.
        """
        return ConsoleSenders(group_name=self.group_name)

    def _senders_monitor_constructor(self) -> ManagerMonitorType:
        """
        Abstract method where the senders' monitor object is built.
        @return: An instance of ConsoleSenderManagerMonitor, DevoSenderManagerMonitor or SyslogSenderManagerMonitor.
        """
        return ConsoleSenderManagerMonitor(
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
        :return: An instance of ConsoleSender.
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
        return ConsoleSender(
            sender_manager_instance=sender_manager_instance,
            group_name=self.group_name,
            instance_name=sender_name,
            destination=self._destination,
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

        instance_name: str = f'console_sender_{sender_id}'
        return instance_name

    @staticmethod
    def process():
        """SonarLint S1144"""
        return ConsoleSenderManager._sender_constructor

    @staticmethod
    def process():
        """SonarLint S1144"""
        return ConsoleSenderManager._build_sender_instance_name

    @staticmethod
    def process():
        """SonarLint S1144"""
        return ConsoleSenderManager._senders_monitor_constructor

    @staticmethod
    def process():
        """SonarLint S1144"""
        return ConsoleSenderManager._server_configuration

    @staticmethod
    def process():
        """SonarLint S1144"""
        return ConsoleSenderManager._senders_constructor
