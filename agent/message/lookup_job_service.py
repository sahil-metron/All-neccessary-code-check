import inspect
import logging
import os
from abc import ABC, abstractmethod
from time import time
from typing import Union

from devo.sender import Sender, SenderConfigSSL, Lookup, DevoSenderException

from agent.message.lookup_job_factory import LookupJob, LookupSettings


class BaseInterface(ABC):
    """ Class base to create Job Interfaces """

    def __init__(self, **kwargs):
        """ Builder """
        pass

    def execute_job(self, lookup_job: LookupJob):
        """
        Execute the job to do.
        :param lookup_job: a LookupJob instance.
        """
        # Validate
        action = lookup_job.settings.action
        assert action in ['INITIALIZE', 'REMOVE', 'MODIFY']

        if action == 'INITIALIZE':
            self._execute_initialize_job(lookup_job)
        elif action == 'REMOVE':
            self._execute_remove_job(lookup_job)
        elif action == 'MODIFY':
            self._execute_update_job(lookup_job)

    @abstractmethod
    def _execute_initialize_job(self, lookup_job: LookupJob):
        """
        Execute a CREATE type job.
        :param lookup_job: a LookupJob instance.
        """
        pass

    @abstractmethod
    def _execute_remove_job(self, lookup_job: LookupJob):
        """
        Execute a CREATE type job.
        :param lookup_job: a LookupJob instance.
        """
        pass

    @abstractmethod
    def _execute_update_job(self, lookup_job: LookupJob):
        """
        Execute a CREATE type job.
        :param lookup_job: a LookupJob instance.
        """
        pass


class DevoInterface(BaseInterface):
    """ Class based on BaseInterface instance for Devo SDK interface """

    def __init__(self, **kwargs):
        """
        Builder
        :param kwargs:    -> devo_con (Sender):  Devo connection
        """
        super().__init__(**kwargs)

        # Validates devo_con and initiate self.devo_con_status
        self.injected_devo_con = kwargs.get('devo_con')
        msg_from = f'[OUTPUT] {__class__.__name__}::{inspect.stack()[0][3]} ->'
        if 'devo_con' not in kwargs:
            logging.warning(
                f'{msg_from} devo_con was not injected in the constructor so it will be created by DevoInterface.')
        elif self.injected_devo_con is None or not isinstance(self.injected_devo_con, Sender):
            self.injected_devo_con = None
            logging.warning(
                f'{msg_from} devo_con was injected with an invalid type so it will be created by DevoInterface.')

    def _execute_initialize_job(self, lookup_job: LookupJob):
        """
        Execute a CREATE type job.
        :param lookup_job: a LookupJob instance.
        """
        self._transfer_items_to_devo(lookup_job=lookup_job, control='FULL')

    def _execute_remove_job(self, lookup_job: LookupJob):
        """
        Execute a REMOVE type job.
        :param lookup_job: a LookupJob instance.
        """
        self._transfer_items_to_devo(lookup_job=lookup_job, control='INC', delete=True)

    def _execute_update_job(self, lookup_job: LookupJob):
        """
        Execute a REMOVE type job.
        :param lookup_job: a LookupJob instance.
        """
        self._transfer_items_to_devo(lookup_job=lookup_job, control='INC')

    def _transfer_items_to_devo(self, lookup_job: LookupJob, control: str, delete: bool = False):
        """
        Method that transfer all lookups to devo
        :param lookup_job: a LookupJob Instance.
        :param control: a valid devo control: INC or FULL.
        :param delete: True if the item will be deleted, False if not. False as default.
            If True, control should be 'INC'.
        """
        msg_from = f'[OUTPUT] {__class__.__name__}::{inspect.stack()[0][3]} ->'

        # Validations
        assert control in ['INC', 'FULL']
        if delete:
            assert control == 'INC'

        # Create devo channel
        settings: LookupSettings = lookup_job.settings
        max_attempts: int = 3
        attempts: int = 0

        while True:
            try:
                devo_con: Sender = self._create_devo_connection()

            except DevoSenderException:
                attempts += 1
                logging.error(f'{msg_from} DevoSenderException found on attempt {attempts}/{max_attempts}')
                if attempts == max_attempts:
                    raise

            else:
                break

        idx_type_of_key = settings.headers.index(settings.key)
        type_of_key = settings.field_types[idx_type_of_key]
        lookup = Lookup(name=settings.lookup_name, historic_tag=settings.historic_tag, con=devo_con)
        p_headers = Lookup.list_to_headers(headers=settings.headers,
                                           type_of_key=type_of_key,
                                           key=settings.key,
                                           types=settings.field_types)
        key_id = settings.headers.index(settings.key)

        logging.debug(f'{msg_from} Transfer started')
        start_epoch: int = int(time())

        # Say hello!
        if settings.send_start:
            lookup.send_control('START', p_headers, control)

        # Say what you want to say!
        for item in lookup_job.to_do:
            if delete and len(item) == 1:
                lookup.send_data_line(
                    key=item[0],
                    delete=delete
                )

            else:
                lookup.send_data_line(
                    key=item[key_id],
                    fields=item,
                    delete=delete
                )

        # Say bye bye!
        if settings.send_end:
            lookup.send_control('END', p_headers, control)

        # Destroy the world! only when the connection has not been injected.
        if self.injected_devo_con is None:
            logging.debug(f"{msg_from} Executed devo_con.close")
            devo_con.close()

        # Debug
        end_epoch: int = int(time())
        elapsed_in_seconds: int = end_epoch - start_epoch
        lookup_events: int = len(lookup_job.to_do)
        logging.debug(f'{msg_from} Job completed: {lookup_events} events sent in {elapsed_in_seconds} secs '
                      f'({round(lookup_events/elapsed_in_seconds,1)} events/sec)')

    @staticmethod
    def _create_sender_config_ssl() -> SenderConfigSSL:
        """
        Returns a SenderConfigSSL object based on the environment settings.
        :return a SenderConfigSSL (devo) instance.
        """

        server = os.getenv('DEVO_SERVER')
        port = int(os.getenv('DEVO_PORT'))
        key = os.getenv('DEVO_SENDER_KEY')
        cert = os.getenv('DEVO_SENDER_CERT')
        chain = os.getenv('DEVO_SENDER_CHAIN')

        engine_config: SenderConfigSSL = SenderConfigSSL(
            address=(server, port),
            key=key,
            cert=cert,
            chain=chain
        )
        return engine_config

    def _create_devo_connection(self) -> Sender:
        """
        Method that creates a ready-to-use Devo sender if not previously injected.
        :return: a Sender (devo) instance.
        """

        if self.injected_devo_con:
            devo_con: Sender = self.injected_devo_con
        else:
            engine_config: SenderConfigSSL = self._create_sender_config_ssl()
            devo_con: Sender = Sender(engine_config)

        return devo_con


JobInterfaces = Union[BaseInterface]


class LookupJobService(object):

    def __init__(self, job_interface: JobInterfaces):
        """ Builder """
        self.job_interface: JobInterfaces = job_interface

    def execute_job(self, lookup_job: LookupJob):
        """
        Execute the job with the loaded job interface
        :param lookup_job: a LookupJob instance.
        """
        self.job_interface.execute_job(lookup_job=lookup_job)


def lookup_job_service_factory_for_devo_interface(**kwargs) -> LookupJobService:
    return LookupJobService(job_interface=DevoInterface(**kwargs))
