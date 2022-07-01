import inspect
import json
import logging
import os
import socket
import threading
from collections import deque
from datetime import datetime
from queue import Empty
from statistics import mean
from typing import Any, Mapping, Optional

from devo.sender import SenderConfigSSL, Sender

from agent.message.lookup_job_factory import LookupJob
from agent.message.lookup_job_service import lookup_job_service_factory_for_devo_interface
from agent.message.message import Message
from agent.outputs.exceptions.exceptions import DevoSenderException
from agent.outputs.senders.abstracts.sender_abstract import SenderAbstract

log = logging.getLogger(__name__)


class DevoSender(SenderAbstract):
    """ Console Sender Class that send the msg by Devo Tunnels """

    def __init__(self, **kwargs: Optional[Any]):
        """Builder - Abstract-base mandatory arguments are marked with (*)

        :param kwargs:  -> (*) group_name (str): Group name
                        -> (*) instance_name (str): Name of the instance
                        -> (*) content_queue (CollectorInternalThreadQueue): Internal Queue used by the sender
                        -> (*) generate_collector_details (bool): Defines if the collector details must be generated
                        -> address (str): IP address
                        -> port (int): Port number
                        -> transport_layer_type (str): Transport layer type (SSL, other)
                        -> chain_path (str): Path to the chain file for the SSL tunnel
                        -> cert_path (str): Path to the certificate file for the SSL tunnel
                        -> key_path (str): Path to the private key file for the SSL tunnel

                        -> connecting_retries (int): Max number of connection retries
                        -> connecting_max_wait_in_seconds (int): Maximum wait time to be used in connection attempts (sec)
                        -> connecting_initial_wait_in_seconds (int): Initial wait time in seconds

                        -> sending_timeout (int): Timeout for sending process
                        -> sending_retries (int): Max number of sending retires
                        -> sending_max_wait_in_seconds (int): (sec)
                        -> sending_initial_wait_in_seconds (int): (sec)

                        -> threshold_for_using_gzip_in_transport_layer (int): Threshold for compression activation
                        -> compression_level (int): Level of the used compression
                        -> compression_buffer_in_bytes (int): Size of the used compression buffer
        """
        super(DevoSender, self).__init__(**kwargs)

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

        transport_layer_type: str = kwargs['transport_layer_type']
        chain_path: str = kwargs['chain_path']
        cert_path: str = kwargs['cert_path']
        key_path: str = kwargs['key_path']
        threshold_for_using_gzip_in_transport_layer: float = kwargs['threshold_for_using_gzip_in_transport_layer']
        compression_level: int = kwargs['compression_level']
        compression_buffer_in_bytes: int = kwargs['compression_buffer_in_bytes']

        # Sender properties
        self._address: str = address
        self._port: int = port
        self._transport_layer_type: str = transport_layer_type
        self._chain_path: str = chain_path
        self._cert_path: str = cert_path
        self._key_path: str = key_path

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

        self._threshold_for_using_gzip_in_transport_layer: float = threshold_for_using_gzip_in_transport_layer
        self._compression_level: int = compression_level
        self._compression_buffer_in_bytes: int = compression_buffer_in_bytes

        # Internal properties
        self._sending_zipped: bool = False

        # Id settings
        self.internal_name: str = 'devo_sender'

        # Sender
        self.sender: Optional[Sender] = None
        self.sender_usages: deque = deque(maxlen=1000)

    def _validate_kwargs_for_method__init__(self, kwargs: Mapping[str, Any]):
        """
        Method that raises exceptions when the kwargs are invalid.
        @param kwargs:  -> address (str): IP address
                        -> port (int): Port number
                        -> transport_layer_type (str): Transport layer type (SSL, other)
                        -> chain_path (str): Path to the chain file for the SSL tunnel
                        -> cert_path (str): Path to the certificate file for the SSL tunnel
                        -> key_path (str): Path to the private key file for the SSL tunnel
                        -> connecting_retries (int): Max number of connection retries
                        -> connecting_max_wait_in_seconds (int): (sec)
                        -> connecting_initial_wait_in_seconds (int): (sec)
                        -> sending_timeout (int): Timeout for sending process
                        -> sending_retries (int): Max number of sending retires
                        -> sending_wait_between_retries_in_seconds (int): Wait time between sending attempts (sec)
                        -> threshold_for_using_gzip_in_transport_layer (float): Threshold for compression activation
                        -> compression_level (int): Level of the used compression
                        -> compression_buffer_in_bytes (int): Size of the used compression buffer
        @raise: AssertionError and ValueError.
        """
        super(DevoSender, self)._validate_kwargs_for_method__init__(kwargs)

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

        # Validate compression_buffer_in_bytes
        val_str = 'compression_buffer_in_bytes'
        assert val_str in kwargs, f'{error_msg} The <{val_str}> argument is mandatory.'
        val_value = kwargs[val_str]

        msg = f'{error_msg} The <{val_str}> must be an instance of <int> type not <{type(val_value)}>'
        assert isinstance(val_value, int), msg

        msg = f'{error_msg} The <{val_str}> value must be between 1KB 10MB (10485760 Bytes)'
        assert 1 <= val_value <= 10485760, msg

        # Validate compression_level

        val_str = 'compression_level'
        assert val_str in kwargs, f'{error_msg} The <{val_str}> argument is mandatory.'
        val_value = kwargs[val_str]

        msg = f'{error_msg} The <{val_str}> must be an instance of <int> type not <{type(val_value)}>'
        assert isinstance(val_value, int), msg

        msg = f'{error_msg} The <{val_str}> value must be between 1KB 10MB (10485760 Bytes)'
        assert 1 <= val_value <= 9, msg

        # Validate transport_layer_type

        val_str = 'transport_layer_type'
        assert val_str in kwargs, f'{error_msg} The <{val_str}> argument is mandatory.'
        val_value = kwargs[val_str]

        msg = f'{error_msg} The <{val_str}> must be an instance of <str> type not <{type(val_value)}>'
        assert isinstance(val_value, str), msg

        values = ['SSL', 'TCP']
        msg = f'{error_msg} The <{val_str}> value must be one of <{values}>'
        assert val_value in values, msg

        # Validate threshold_for_using_gzip_in_transport_layer
        val_str = 'threshold_for_using_gzip_in_transport_layer'
        assert val_str in kwargs, f'{error_msg} The <{val_str}> argument is mandatory.'
        val_value = kwargs[val_str]

        msg = f'{error_msg} The <{val_str}> must be an instance of <float> or <int> type not <{type(val_value)}>'
        assert isinstance(val_value, float) or isinstance(val_value, int), msg

        msg = f'{error_msg} The <{val_str}> value must be between -1.0 and 1.1'
        assert -1 <= val_value <= 1.1, msg

        # Validate chain_path
        val_str = 'chain_path'
        assert val_str in kwargs, f'{error_msg} The <{val_str}> argument is mandatory.'
        val_value = kwargs[val_str]

        msg = f'{error_msg} The <{val_str}> must be an instance of <str> type not <{type(val_value)}>'
        assert isinstance(val_value, str), msg

        msg = f'{error_msg} The <{val_str}> must be a valid path'
        assert os.path.exists(val_value), msg

        # Validate cert_path

        val_str = 'cert_path'
        assert val_str in kwargs, f'{error_msg} The <{val_str}> argument is mandatory.'
        val_value = kwargs[val_str]

        msg = f'{error_msg} The <{val_str}> must be an instance of <str> type not <{type(val_value)}>'
        assert isinstance(val_value, str), msg

        msg = f'{error_msg} The <{val_str}> must be a valid path'
        assert os.path.exists(val_value), msg

        # Validate key_path

        val_str = 'key_path'
        assert val_str in kwargs, f'{error_msg} The <{val_str}> argument is mandatory.'
        val_value = kwargs[val_str]

        msg = f'{error_msg} The <{val_str}> must be an instance of <str> type not <{type(val_value)}>'
        assert isinstance(val_value, str), msg

        msg = f'{error_msg} The <{val_str}> must be a valid path'
        assert os.path.exists(val_value), msg

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

        # This exception is raised when no message is coming to the SenderManager
        # queue by the established timeout (10 seconds)
        except Empty:
            if self.sender:
                self.sender.flush_buffer()
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
            log.info(f"Closing connection to Devo")
            try:
                self.sender.close()
            except:
                pass

            self.sender = None

    def _get_connection_status(self) -> bool:
        """
        Method that returns the sender connection status.
        @return: True if the connection is working. False if not.
        @example:
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
        """Method that processes the lookup type messages.

        :param lookup: LookupJob to be processed.
        :param start_time: Time when the process was initialized.
        """
        lookup_has_been_sent = self._send_lookup_message(lookup)
        self.last_usage_timestamp = datetime.utcnow()
        if lookup_has_been_sent is True:
            if self._generate_collector_details:
                self._sender_stats.increase_lookup_counter((datetime.utcnow() - start_time).total_seconds())
        return lookup_has_been_sent

    def create_sender_if_not_exists(self):
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
            f'"url": "{self._address}:{self._port}", ' \
            f'"chain_path": "{self._chain_path}", ' \
            f'"cert_path": "{self._cert_path}", ' \
            f'"key_path": "{self._key_path}", ' \
            f'"transport_layer_type": "{self._transport_layer_type}"' \
            f'}}'

    def _send_standard_message(self, message: Message) -> bool:
        """Method that sends a new standard message to Devo.

        :param message: message object
        :return: True if the message is sent properly. Nothing if not.
        """

        message_has_been_sent: bool = False

        # Check if a sender is available
        if self.sender is None:
            self.sender = self._create_sender()

        if self.sender:

            zipped, queue_usage = self.__should_zipped_sending_be_enabled()

            # Log the zipping feature status when it changes
            self._log_zipping_feature_changes(zipped, queue_usage)

            message_has_been_sent = self._transmit_standard_message(message, zipped)

        return message_has_been_sent

    def __should_zipped_sending_be_enabled(self) -> (bool, float):
        """

        :return:
        """
        queue_usage: float = 0.0
        if self._threshold_for_using_gzip_in_transport_layer < 0:
            zipped = True
        else:
            # The zipping feature should be used?
            zipped = False
            queue_usage = self._content_queue.percentage_usage()
            self.sender_usages.append(queue_usage)
            if len(self.sender_usages) == self.sender_usages.maxlen:
                queue_usage = mean(self.sender_usages)
                if queue_usage >= self._threshold_for_using_gzip_in_transport_layer:
                    zipped = True

        return zipped, queue_usage

    def _log_zipping_feature_changes(self, zipped: bool, queue_usage: float):
        """Method that sends to the log a message when the zipping feature start or ends to be used.

        :param zipped: False if the zipping feature should be used. True if not.
        :return:
        """
        # Compare current and new statuses
        if self._sending_zipped != zipped:
            if self._threshold_for_using_gzip_in_transport_layer < 0:
                log.info(
                    'The threshold for using the zipped transport layer '
                    'has been set less than 0 so it will be used always'
                )

            self._sending_zipped = zipped

            if zipped:
                log.debug(
                    f'Internal queue is being used more than '
                    f'{self._threshold_for_using_gzip_in_transport_layer:.0%} ({queue_usage:.2%}) > '
                    f'Changing to sending zipped')
            else:
                if self.sender:
                    self.sender.flush_buffer()

                log.debug(
                    f'Internal queue is being used less than '
                    f'{self._threshold_for_using_gzip_in_transport_layer:.0%} ({queue_usage:.2%}) > '
                    f'Changing to sending unzipped (previous buffer will be flushed)')

    def _send_lookup_message(self, lookup: LookupJob) -> bool:
        """Method that sends a new lookup job to devo.

        :param lookup: lookup object
        :return: True if the message is sent properly. Nothing if not.
        """

        message_has_been_sent: bool = False

        try:
            if self.sender is None:
                self.sender = self._create_sender()

            if self.sender:
                message_has_been_sent = self._transmit_lookup_job(lookup)

        except Exception as ex:
            log.error(ex)

        return message_has_been_sent

    def _create_sender(self) -> Sender:
        """Method that creates a new real sender.

        :return: Sender object.
        """
        # Initial configurations
        sender = None
        current_retry_counter: int = 0
        previous_wait_time_in_seconds: int = 0
        last_exception = None

        # Create the connection
        while current_retry_counter < self._connecting_retries and sender is None:
            try:
                # The "sec_level" must have value 1 because the certificates used are "too weak"
                sender_config = SenderConfigSSL(
                    address=(self._address, self._port),
                    chain=self._chain_path,
                    cert=self._cert_path,
                    key=self._key_path,
                    check_hostname=False,
                    sec_level=1
                )

                sender = Sender(config=sender_config)

                sender.buffer_size(size=self._compression_buffer_in_bytes)
                sender.compression_level(self._compression_level)

                sender_session = sender.socket.session if sender else None
                log.info(
                    f'Created a sender: {self}, '
                    f'hostname: "{sender_config.hostname}", '
                    f'session_id: "{id(sender_session)}"')

            except ConnectionRefusedError as ex:
                last_exception = self._handle_sender_connection_refused_error(ex)

            except Exception as ex:
                last_exception = self._handle_sender_generic_exception(ex)

            # Troubleshooting
            if sender is None:
                if previous_wait_time_in_seconds == 0:
                    wait_time = self._sending_initial_wait_in_seconds
                else:
                    wait_time = previous_wait_time_in_seconds ** 2
                if wait_time > self._connecting_max_wait_in_seconds:
                    wait_time = self._connecting_max_wait_in_seconds

                log.warning(
                    f'Some error happened when creating sender, waiting {wait_time} seconds for a retry'
                )
                self._connecting_retry_wait_object.wait(timeout=wait_time)
                previous_wait_time_in_seconds = wait_time if wait_time > 1 else 2

            current_retry_counter += 1

        # Troubleshooting
        if current_retry_counter == self._connecting_retries and sender is None:
            exception_dict: dict = {
                "max_retries": self._connecting_retries,
                "exception_type": str(type(last_exception)),
                "exception": str(last_exception)
            }
            log.error(
                f'Connecting retries limit reached, '
                f'details: {json.dumps(exception_dict)}'
            )

        return sender

    def _transmit_standard_message(self, message: Message, zipped: bool) -> bool:
        """Send the given message object by syslog through the created sender.

        :param message: message object
        :param zipped: Define if the zipping feature must be used
        :return: Tuple (bool, Exception) with the following values (message_has_been_sent, last_exception)
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
                message_date = message.timestamp
                _ = message_date

                if zipped:
                    message_content_bytes = bytes(message.content, "utf8")
                    message_tag_bytes = bytes(message.tag, "utf-8")
                    number_of_sent_items = \
                        self.sender.send_bytes(message_tag_bytes, message_content_bytes, zip=zipped)
                    # If buffer is not full, nothing is sent, but it is put inside buffer
                    message_has_been_sent = True
                    current_retry_counter = 0
                else:
                    number_of_sent_items = self.sender.send(message.tag, message.content)
                    if number_of_sent_items > 0:
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

    def _transmit_lookup_job(self, lookup_job: LookupJob) -> bool:
        """Send the given lookup object by syslog through the created sender.

        :param lookup_job: lookup object
        :return:
        """
        # Required settings
        message_has_been_sent = False
        current_retry_counter: int = 0
        previous_wait_time_in_seconds: int = 0
        last_exception = None

        # Send lookup
        while current_retry_counter < self._sending_retries and message_has_been_sent is False:
            try:
                self.create_sender_if_not_exists()
                kwargs = {'devo_con': self.sender}
                lookup_service = lookup_job_service_factory_for_devo_interface(**kwargs)
                lookup_service.execute_job(lookup_job=lookup_job)
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
        if current_retry_counter == self._sending_retries and message_has_been_sent is False:
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

    def _handle_sender_connection_refused_error(self, exception_details: Exception) -> DevoSenderException:
        """
        Method that creates a new exception object with the given connection refused error content and return it.
        @param exception_details: Caught exception
        @return: A new DevoSenderException object.
        """
        return DevoSenderException(
            7,
            "Connection has been refused: "
            "{{"
            "\"address\": \"{address}\", "
            "\"port\": {port}, "
            "\"type\": \"{transport_layer_type}\", "
            "\"chain_path\": {chain}, "
            "\"cert_path\": {cert}, "
            "\"key_path\": {key}, "
            "\"exception\": \"{exception}\""
            "}}".format(
                address=self._address,
                port=self._port,
                transport_layer_type=self._transport_layer_type,
                chain="\"{}\"".format(self._chain_path) if self._chain_path else "null",
                cert="\"{}\"".format(self._cert_path) if self._cert_path else "null",
                key="\"{}\"".format(self._key_path) if self._key_path else "null",
                exception=exception_details
            )
        )

    def _handle_sender_generic_exception(self, exception_details: Exception) -> DevoSenderException:
        """
        Method that creates a new exception object with the given error content and return it
        @param exception_details: Caught exception
        @return: A new DevoSenderException object.
        """
        return DevoSenderException(
            8,
            "Error: "
            "{{"
            "\"address\": \"{address}\", "
            "\"port\": {port}, "
            "\"type\": \"{transport_layer_type}\", "
            "\"chain_path\": {chain}, "
            "\"cert_path\": {cert}, "
            "\"key_path\": {key}, "
            "\"exception_type\": \"{exception_type}\""
            "\"exception\": \"{exception}\""
            "}}".format(
                address=self._address,
                port=self._port,
                transport_layer_type=self._transport_layer_type,
                chain="\"{}\"".format(self._chain_path) if self._chain_path else "null",
                cert="\"{}\"".format(self._cert_path) if self._cert_path else "null",
                key="\"{}\"".format(self._key_path) if self._key_path else "null",
                exception_type=type(exception_details),
                exception=exception_details
            )
        )

    def check_if_sender_should_be_disabled(self) -> None:
        """Method that detects senders that should be disabled.

        :return:
        """

        if self.last_usage_timestamp and self.sender:
            usage_elapsed_seconds = (datetime.utcnow() - self.last_usage_timestamp).total_seconds()
            if usage_elapsed_seconds <= 60:
                return

            log.info(
                f'This sender has not been used for {usage_elapsed_seconds} seconds, '
                f'it will be closed and destroyed'
            )

            try:
                self.sender.close()
            except OSError:
                pass
            finally:
                self.sender = None
                self.last_usage_timestamp = None
