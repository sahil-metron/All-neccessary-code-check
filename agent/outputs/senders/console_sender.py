import inspect
import logging
from datetime import datetime
from queue import Empty
from typing import Any, Mapping, Optional

import pysyslogclient

from agent.message.lookup_job_factory import LookupJob
from agent.message.message import Message
from agent.outputs.senders.abstracts.sender_abstract import SenderAbstract

log = logging.getLogger(__name__)


class ConsoleSender(SenderAbstract):
    """ Console Sender Class that send the msg to the console """

    def __init__(self, **kwargs: Optional[Any]):
        """
        Builder - Abstract-base mandatory arguments are marked with (*)
        :param kwargs:  -> (*) group_name (str): Group name
                        -> (*) instance_name (str): Name of the instance
                        -> (*) internal_queue (CollectorInternalThreadQueue): Collector Internal Queue
                        -> (*) generate_collector_details (bool): Defines if the collector details must be generated
                        -> destination (str): Console destination.
        """
        super(ConsoleSender, self).__init__(**kwargs)

        # Kwargs loading
        destination: str = kwargs['destination']

        # Id settings
        self.internal_name: str = 'console_sender'

        # Sender
        self.sender: Optional[pysyslogclient.SyslogClientRFC3164] = None
        self._destination = destination

    def _validate_kwargs_for_method__init__(self, kwargs: Mapping[str, Any]):
        """
        Method that raises exceptions when the kwargs are invalid.
        :param kwargs:  -> destination (str): Destination of the message
        :raise: AssertionError and ValueError.
        """
        super(ConsoleSender, self)._validate_kwargs_for_method__init__(kwargs)

        error_msg = f"[INTERNAL LOGIC] {__class__.__name__}::{inspect.stack()[0][3]} ->"

        # Validate destination

        val_str = 'destination'
        assert val_str in kwargs, f'{error_msg} The <{val_str}> argument is mandatory.'
        val_value = kwargs[val_str]

        msg = f'{error_msg} The <{val_str}> must be an instance of <str> or None, <{type(val_value)}>'
        assert isinstance(val_value, str) or val_value is None, msg

        # msg = f'{error_msg} The <{val_str}> length must be between 1 and 50 not <{len(val_value)}>'
        # assert len(val_value) >= 1, msg
        # assert len(val_value) <= 50, msg

    def _run_try_catch(self):
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

    def _run_finalization(self):
        """ Method not required for this implementation """
        pass

    def _get_connection_status(self) -> bool:
        """
        Method that returns the sender connection status.
        :return: True if the connection is working. False if not.
        :example:
        is_connection_open = True if self.sender else False
        return is_connection_open
        """
        return True

    def _process_message(self, message: Message, start_time: datetime):
        """Method that processes the general types of messages.

        :param message: Message to be processed.
        :param start_time: Time when the process was initialized.
        """
        if self._destination in ["standard", "console", "stdout"]:
            log.info(f"{message}")
        if message.is_internal is True:
            self._sender_stats.increase_internal_messages_counter((datetime.utcnow() - start_time).total_seconds())
        else:
            self._sender_stats.increase_standard_messages_counter((datetime.utcnow() - start_time).total_seconds())

    def _process_lookup(self, lookup: LookupJob, start_time: datetime) -> bool:
        """Method that processes the lookup type messages.
        :param lookup: Lookup to be processed.
        :param start_time: Time when the process was initialized.
        """
        if self._destination in ["standard", "console", "stdout"]:
            log.info(f"{lookup}")
        self._sender_stats.increase_lookup_counter((datetime.utcnow() - start_time).total_seconds())

    def _create_sender_if_not_exists(self):
        """ Method that creates new senders if not exist """

        log.debug(f"{self.name} -> Sender connection does not needed")

    def __str__(self):
        """ Returns a string representation of the class """
        return \
            f'{{' \
            f'"group_name": "{self.group_name}", ' \
            f'"instance_name": "{self.instance_name}", ' \
            f'"destination": {self._destination}' \
            f'}}'

    def create_sender_if_not_exists(self):
        """ Method that create a senders if not exist """

        log.debug(f"{self.name} -> Sender connection does not needed")
