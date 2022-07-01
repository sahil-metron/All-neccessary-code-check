import inspect
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Union, Mapping, Any

from agent.controllers.statuses.status_enum import StatusEnum
from agent.outputs.senders.console_sender import ConsoleSender
from agent.outputs.senders.devo_sender import DevoSender
from agent.outputs.senders.syslog_sender import SyslogSender

SenderType = Union[ConsoleSender, DevoSender, SyslogSender]

log = logging.getLogger(__name__)


class SendersAbstract(ABC):
    """ Senders abstract Class to be used as a base """

    def __init__(self, **kwargs):
        """
        Builder
        @param kwargs:  -> group_name (str): Group name assigned to the senders.
        """
        self.validate_kwargs_for_method__init__(kwargs)

        # kwargs loading
        group_name = kwargs['group_name']

        # Id Settings
        self.group_name = group_name

        # Senders Settings
        self._senders = []
        self._next_sender_index: int = 0
        self._number_of_started_senders: int = 0

    @staticmethod
    def validate_kwargs_for_method__init__(kwargs: Mapping[str, Any]):
        """
        Method that raises exceptions when the kwargs are invalid.
        @param kwargs:  -> group_name (str): Group name assigned to the senders.
        @raise: AssertionError and ValueError when the validation is not passed.
        """
        error_msg = f"[INTERNAL LOGIC] {__class__.__name__}::{inspect.stack()[0][3]} ->"

        # Validate group_name
        validate_name = 'group_name'
        assert validate_name in kwargs, f'{error_msg} The <{validate_name}> argument is mandatory.'
        validate_content = kwargs['group_name']

        msg = f'{error_msg} The <{validate_name}> must be an instance of <str> type not <{type(kwargs[validate_name])}>'
        assert isinstance(validate_content, str), msg

        msg = f'{error_msg} The <{validate_name}> length must be between 1 and 20 not <{len(validate_content)}>'
        assert len(validate_content) > 0, msg
        assert len(validate_content) < 21, msg

    def add_sender(self, sender: SenderType):
        """
        Method that adds a new sender to the senders array.
        @param sender: Sender object that will be appended to the array.
        """
        self._senders.append(sender)

    def get_active_senders(self) -> list:
        """Obtain the sender instances that still active

        :return:
        """

        active_instances: list = []

        for sender in self._senders:
            sender: SenderType
            if sender.get_component_status() == StatusEnum.RUNNING:
                active_instances.append(sender)

        return active_instances

    def get_senders(self) -> list:
        """

        :return:
        """
        return self._senders

    def pause_instances(self) -> None:
        """ Pauses all sender instances

        :return:
        """
        for sender in self._senders:
            sender: SenderType
            if sender.is_in_pause() is False:
                sender.pause()
            else:
                log.info(f'Sender {sender.name} already in pause status')

    def start_instances(self):
        """ Method that starts all the sender instances. """
        if self._senders:
            for sender in self._senders:
                sender: SenderType
                sender.start()
                self._number_of_started_senders += 1

    def get_number_of_instances(self) -> int:
        """
        Method that returns the number of sender instances in the array
        @return: Length of the senders array.
        """
        return len(self._senders)

    def get_sender_stats(self, timestamp: datetime) -> list:
        """
        Method that returns the stats of all sender instances.
        @param timestamp: datetime
        @return:
        """
        sender_instances_stats = []
        for sender_instance in self._senders:
            sender_instance: SenderType
            sender_instances_stats.append(sender_instance.get_stats(timestamp))

        return sender_instances_stats

    def stop_instances(self):
        """ Method that stops all instances of the senders array. """
        if self._senders:
            for sender in self._senders:
                sender: SenderType
                sender.stop()
                self._number_of_started_senders -= 1

    def __str__(self):
        return f"{{" \
               f"\"number_of_senders\": {self.get_number_of_instances()}," \
               f" \"number_of_started_senders\": {self._number_of_started_senders}" \
               f"}}"

    @abstractmethod
    def get_next_sender(self, **kwargs) -> SenderType:
        """
        Method that returns the next sender to be used by the manager.
        @param kwargs: Arguments compatibility with all abstractions.
        @return: A Sender Instance
        """
        pass

    def activate_flush_to_persistence_system_mode(self):
        """

        :return:
        """

        if self._senders:
            for sender in self._senders:
                sender: SenderType
                sender.activate_flush_to_persistence_system_mode()
