import inspect
import json
import logging
import socket
import threading
from datetime import datetime
from queue import Empty
from typing import Any, Mapping, Optional

import pysyslogclient
from devo.sender import Lookup

from agent.message.collector_lookup import CollectorLookup
from agent.message.lookup_job_factory import LookupJob
from agent.message.message import Message
from agent.outputs.commons.CustomSyslogClientRFC3164 import CustomSyslogClientRFC3164
from agent.outputs.senders.abstracts.sender_abstract import SenderAbstract

log = logging.getLogger(__name__)


class SyslogSender(SenderAbstract):
    """ Console Sender Class that send the msg by syslog """

    def __init__(self, **kwargs):
        """
        Builder - Abstract-base mandatory arguments are marked with (*)
        :param kwargs:  -> (*) group_name (str): Group name
                        -> (*) instance_name (str): Name of the instance
                        -> (*) internal_queue (CollectorInternalThreadQueue): Internal Queue used by the sender
                        -> (*) generate_collector_details (bool): Defines if the collector details must be generated
                        -> address (str): IP address
                        -> port (int): Port number

                        -> connecting_retries (int): Max number of connection retries
                        -> connecting_wait_between_retries_in_seconds (int): Wait time between connection attempts (sec)

                        -> sending_timeout (int): Timeout for sending process
                        -> sending_retries (int): Max number of sending retires
                        -> sending_max_wait_in_seconds (int): (sec)
                        -> sending_initial_wait_in_seconds (int): (sec)
        """
        super(SyslogSender, self).__init__(**kwargs)

        # Kwargs loading
        address: str = kwargs['address']
        port: int = kwargs['port']

        connecting_retries: int = kwargs['connecting_retries']
        connecting_max_wait_in_seconds: int = kwargs['connecting_max_wait_in_seconds']
        connecting_initial_wait_in_seconds: int = kwargs['connecting_initial_wait_in_seconds']

        sending_timeout: int = kwargs['sending_timeout']
        sending_retries: int = kwargs['sending_retries']
        sending_max_wait_in_seconds: int = kwargs['sending_max_wait_in_seconds']
        sending_initial_wait_in_seconds: int = kwargs['sending_initial_wait_in_seconds']

        # Sender properties
        self._address: str = address
        self._port: int = port

        # Connection properties
        self._connecting_retries: int = connecting_retries
        self._connecting_max_wait_in_seconds: int = connecting_max_wait_in_seconds
        self._connecting_initial_wait_in_seconds: int = connecting_initial_wait_in_seconds
        self._connecting_retry_wait_object: threading.Event = threading.Event()

        # Sending properties
        self._sending_timeout: int = sending_timeout
        self._sending_retries: int = sending_retries
        self._sending_max_wait_in_seconds: int = sending_max_wait_in_seconds
        self._sending_initial_wait_in_seconds: int = sending_initial_wait_in_seconds
        self._sending_retry_wait_object: threading.Event = threading.Event()

        # Id settings
        self.internal_name: str = 'syslog_sender'

        # Sender
        self.sender: Optional[pysyslogclient.SyslogClientRFC3164] = None

    def _validate_kwargs_for_method__init__(self, kwargs: Mapping[str, Any]):
        """
        Method that raises exceptions when the kwargs are invalid.
        :param kwargs:  -> address (str): IP address
                        -> port (int): Port number

                        -> connecting_retries (int): Max number of connection retries
                        -> connecting_max_wait_in_seconds (int): Maximum wait time to be used in connection attempts (sec)
                        -> connecting_initial_wait_in_seconds (int): Initial wait time in seconds

                        -> sending_timeout (int): Timeout for sending process
                        -> sending_retries (int): Max number of sending retires
                        -> sending_max_wait_in_seconds (int): (sec)
                        -> sending_initial_wait_in_seconds (int): (sec)

        :raise: AssertionError and ValueError.
        """
        super(SyslogSender, self)._validate_kwargs_for_method__init__(kwargs)

        error_msg = f"[INTERNAL LOGIC] {__class__.__name__}::{inspect.stack()[0][3]} ->"

        # Validate address
        val_str = 'address'
        assert val_str in kwargs, f'{error_msg} The <{val_str}> argument is mandatory.'
        val_value = kwargs[val_str]

        msg = f'{error_msg} The <{val_str}> must be an instance of <str> type not <{type(val_value)}>'
        assert isinstance(val_value, str), msg

        msg = f'{error_msg} The <{val_str}> does not appear to be an IP address and cannot be verified: {val_value}'
        try:
            socket.inet_aton(val_value)
        except socket.error:
            log.info(msg)

        # Validate port
        val_str = 'port'
        assert val_str in kwargs, f'{error_msg} The <{val_str}> argument is mandatory.'
        val_value = kwargs[val_str]

        msg = f'{error_msg} The <{val_str}> must be an instance of <int> type not <{type(val_value)}>'
        assert isinstance(val_value, int), msg

        msg = f'{error_msg} The <{val_str}> must be a valid port (1-65535)'
        assert 1 <= val_value <= 65535, msg

        # Validate connecting_retries
        val_str = 'connecting_retries'
        assert val_str in kwargs, f'{error_msg} The <{val_str}> argument is mandatory.'
        val_value = kwargs[val_str]

        msg = f'{error_msg} The <{val_str}> must be an instance of <int> type not <{type(val_value)}>'
        assert isinstance(val_value, int), msg

        msg = f'{error_msg} The <{val_str}> value must be between 1 and 10'
        assert 1 <= val_value <= 10, msg

        # Validate connecting_max_wait_in_seconds
        val_str = 'connecting_max_wait_in_seconds'
        assert val_str in kwargs, f'{error_msg} The <{val_str}> argument is mandatory.'
        val_value = kwargs[val_str]

        msg = f'{error_msg} The <{val_str}> must be an instance of <int> type not <{type(val_value)}>'
        assert isinstance(val_value, int), msg

        msg = f'{error_msg} The <{val_str}> value must be between 1 and 60'
        assert 1 <= val_value <= 60, msg

        # Validate connecting_initial_wait_in_seconds
        val_str = 'connecting_initial_wait_in_seconds'
        assert val_str in kwargs, f'{error_msg} The <{val_str}> argument is mandatory.'
        val_value = kwargs[val_str]

        msg = f'{error_msg} The <{val_str}> must be an instance of <int> type not <{type(val_value)}>'
        assert isinstance(val_value, int), msg

        msg = f'{error_msg} The <{val_str}> value must be between 1 and 60'
        assert 1 <= val_value <= 10, msg

        # Validate sending_timeout
        val_str = 'sending_timeout'
        assert val_str in kwargs, f'{error_msg} The <{val_str}> argument is mandatory.'
        val_value = kwargs[val_str]

        msg = f'{error_msg} The <{val_str}> must be an instance of <int> type not <{type(val_value)}>'
        assert isinstance(val_value, int), msg

        msg = f'{error_msg} The <{val_str}> value must be between 1 and 10'
        assert 1 <= val_value <= 10, msg

        # Validate sending_retries
        val_str = 'sending_retries'
        assert val_str in kwargs, f'{error_msg} The <{val_str}> argument is mandatory.'
        val_value = kwargs[val_str]

        msg = f'{error_msg} The <{val_str}> must be an instance of <int> type not <{type(val_value)}>'
        assert isinstance(val_value, int), msg

        msg = f'{error_msg} The <{val_str}> value must be between 1 and 10'
        assert 1 <= val_value <= 10, msg

        # Validate sending_max_wait_in_seconds
        val_str = 'sending_max_wait_in_seconds'
        assert val_str in kwargs, f'{error_msg} The <{val_str}> argument is mandatory.'
        val_value = kwargs[val_str]

        msg = f'{error_msg} The <{val_str}> must be an instance of <int> type not <{type(val_value)}>'
        assert isinstance(val_value, int), msg

        msg = f'{error_msg} The <{val_str}> value must be between 1 and 60'
        assert 1 <= val_value <= 60, msg

        # Validate sending_initial_wait_in_seconds
        val_str = 'sending_initial_wait_in_seconds'
        assert val_str in kwargs, f'{error_msg} The <{val_str}> argument is mandatory.'
        val_value = kwargs[val_str]

        msg = f'{error_msg} The <{val_str}> must be an instance of <int> type not <{type(val_value)}>'
        assert isinstance(val_value, int), msg

        msg = f'{error_msg} The <{val_str}> value must be between 1 and 10'
        assert 1 <= val_value <= 10, msg

    def _run_try_catch(self) -> None:
        """Method that contains the try & catch structure to be abstracted.

        :example:

            try:
                self._run_execution()
            except Empty:
                pass
            except Exception as ex:
                log.error(f'An error has happen, details: {ex}')
                self._running_flag = False
        """

        try:
            self._run_execution()
        except Empty:
            pass
        except Exception as ex:
            log.error(f'An error has happen, details: {ex}')
            self._running_flag = False

    def _run_finalization(self) -> None:
        """Method used to execute actions when the <finalizing thread status> is reached

        :example:

            if self.sender:
                log.info(f"Closing connection to Syslog server")
                self.sender.close()
                self.sender = None
        """

        if self.sender:
            log.info(f"Closing connection to Syslog server")
            try:
                self.sender.close()
            except:
                pass

            self.sender = None

    def _get_connection_status(self) -> bool:
        """
        Method that returns the sender connection status.
        :return: True if the connection is working. False if not.
        :example:
        is_connection_open = True if self.sender else False
        return is_connection_open
        """
        is_connection_open = True if self.sender else False
        return is_connection_open

    def _process_message(self, message: Message, start_time: datetime) -> bool:
        """Method that processes the general types of messages.

        :param message: Message to be processed.
        :param start_time: Time when the process was initialized.
        """

        message_has_been_sent = self._send_standard_message(message)
        self.last_usage_timestamp = datetime.utcnow()
        if message_has_been_sent is True:
            if message.is_internal is True:
                self._sender_stats.increase_internal_messages_counter((datetime.utcnow() - start_time).total_seconds())
            else:
                self._sender_stats.increase_standard_messages_counter((datetime.utcnow() - start_time).total_seconds())

        return message_has_been_sent

    def _process_lookup(self, lookup: LookupJob, start_time: datetime) -> bool:
        """Method that processes the lookup type messages. Drop all messages. Syslog cannot send Lookups.
        :param lookup: Lookup to be processed.
        :param start_time: Time when the process was initialized.
        """
        log.error(f"Message dropped: {lookup}")
        return False

    def _create_sender_if_not_exists(self):
        """ Method that creates new senders if not exist """

        if self.sender is None:
            log.debug(f"{self.name} -> Sender connection was not existing")
            self.sender = self._create_sender()

    def __str__(self):
        """ Returns a string representation of the class """
        return \
            f'{{' \
            f'"group_name": "{self.group_name}", ' \
            f'"instance_name": "{self.instance_name}", ' \
            f'"url": {self._address}:{self._port}' \
            f'}}'

    def _send_standard_message(self, message: Message) -> bool:
        """Method that sends a new standard message by syslog

        :param message: message object
        :return: True if the message is sent properly. Nothing if not.
        """

        message_has_been_sent: bool = False

        try:
            if self.sender is None:
                self.sender = self._create_sender()

            if self.sender:
                message_has_been_sent = self._transmit_standard_message(message)

        except Exception as ex:
            log.error(ex)

        return message_has_been_sent

    def _create_sender(self) -> pysyslogclient.SyslogClientRFC3164:
        """Method that creates a new real sender.

        :return: Sender object.
        """
        client: pysyslogclient.SyslogClientRFC3164 = CustomSyslogClientRFC3164(self._address, self._port)
        log.info(f"{self.instance_name} -> Created sender: {client}")

        return client

    def _transmit_standard_message(self, message: Message) -> bool:
        """Send the given message object by syslog through the created sender.

        :param message: message object
        :param message:
        :return:
        """

        # Required settings
        message_has_been_sent: bool = False
        number_of_sent_items: int = 0
        current_retry_counter: int = 0
        previous_wait_time_in_seconds: int = 0
        last_exception = None

        # Send the message
        while current_retry_counter < self._sending_retries \
                and message_has_been_sent is False:
            try:
                msg_content = message.content
                self.sender.log(msg_content, program=message.tag)

                message_has_been_sent = True
                current_retry_counter = 0

            except Exception as ex:
                last_exception = ex
                exception_dict: dict = {
                    "exception_type": str(type(ex)),
                    "exception": str(ex)
                }
                log.debug(f'Error sending: {json.dumps(exception_dict)}')

                current_retry_counter += 1

            # Troubleshooting
            if message_has_been_sent is False:
                if previous_wait_time_in_seconds == 0:
                    wait_time = self._sending_initial_wait_in_seconds
                else:
                    wait_time = previous_wait_time_in_seconds * 2
                self._sending_retry_wait_object.wait(timeout=wait_time)
                previous_wait_time_in_seconds = wait_time

        # Troubleshooting
        if current_retry_counter == self._sending_retries \
                and message_has_been_sent is False:
            exception_dict: dict = {
                "max_retries": self._sending_retries,
                "exception_type": str(type(last_exception)),
                "exception": str(last_exception)
            }
            log.error(
                f'Sending retries limit reached, message was not sent, '
                f'details: {json.dumps(exception_dict)}'
            )

        return message_has_been_sent

    @staticmethod
    def _run_lookup_logic(collector_lookup: CollectorLookup, devo_lookup: Lookup):
        """
        Method that runs the lookup logic called from _send_lookup_to_syslog_server and created to reduce the
        cyclomatic complexity of this.
        @param collector_lookup: Lookup object from the queue.
        @param devo_lookup: Lookup object ready to be sent to Devo Relay.
        """
        if collector_lookup.executed_start is False:
            log.debug("LU -> Sending \"START\"")
            devo_lookup.send_control(event="START", headers=collector_lookup.headers,
                                     action=collector_lookup.action_type)
            collector_lookup.executed_start = True
            log.debug("LU -> \"START\" sent")

        if collector_lookup.executed_end is False and collector_lookup.is_empty() is False:
            while collector_lookup.is_empty() is False:
                lookup_rows, lookup_row_is_delete_action = collector_lookup.get_next_data_group()
                for lookup_row in lookup_rows:
                    devo_lookup.send_data(row=lookup_row, delete=lookup_row_is_delete_action)
                    log.debug(f"LU -> Row: delete: {lookup_row_is_delete_action}, data_sent: [{lookup_row}]")

        if collector_lookup.execute_end is True and collector_lookup.executed_end is False:
            while collector_lookup.is_empty() is False:
                lookup_rows, lookup_row_is_delete_action = collector_lookup.get_next_data_group()
                for lookup_row in lookup_rows:
                    devo_lookup.send_data(row=lookup_row, delete=lookup_row_is_delete_action)
                    log.debug(f"LU -> Row: delete: {lookup_row_is_delete_action}, data_sent: [{lookup_row}]")

            log.debug("LU -> Sending \"END\"")
            devo_lookup.send_control(event="END", headers=collector_lookup.headers, action=collector_lookup.action_type)
            collector_lookup.executed_end = True
            log.debug("LU -> \"END\" sent")

    def create_sender_if_not_exists(self):
        """ Method that create a senders if not exist """
        if self.sender is None:
            log.debug(f"{self.name} -> Sender connection was not existing")
            self.sender = self._create_sender()
