from typing import Any, Optional

from agent.outputs.senders.abstracts.senders_abstract import SendersAbstract, SenderType
from agent.outputs.senders.devo_sender import DevoSender


class DevoSenders(SendersAbstract):
    """ Senders Abstraction for Devo """

    def __init__(self, **kwargs: Optional[Any]):
        """Builder

        :param kwargs:  group_name: str -> Group name
                        threshold: float -> Threshold to decide when the second connection will be used if exists
        """
        super().__init__(group_name=kwargs['group_name'])

        # Layer settings
        self.__threshold = kwargs.get('threshold', 0.75)

    def get_next_sender(self, **kwargs: Optional[Any]) -> SenderType:
        """
        Method that returns the next sender to be used by the manager taking in consideration the queue usage and the
        threshold.
        @param kwargs:  queue_usage_percentage: float -> Percentage of the queue usage
        @return: An instance of a sender.
        @raise: An ValueError when there is no sender available.
        """
        self.validate_kwargs_for_method_get_next_sender(kwargs)

        queue_usage_percentage: float = kwargs['queue_usage_percentage']

        if len(self._senders) > 0:
            # Activate the alternative senders only if the threshold has been reached.
            if queue_usage_percentage > self.__threshold:
                sender: DevoSender = self._senders[self._next_sender_index]
                self._next_sender_index += 1
                if self._next_sender_index >= len(self._senders):
                    self._next_sender_index = 0
            else:
                # When the threshold has not been reached use always the first one.
                sender: DevoSender = self._senders[0]

            self.check_if_senders_should_be_disabled()
            sender.create_sender_if_not_exists()

            return sender

        raise ValueError('[SyslogSenders] get_next_sender(method) -> No senders instance available')

    @staticmethod
    def validate_kwargs_for_method_get_next_sender(payload: dict):
        """
        Method that raises exceptions when the kwargs are invalid.
        @param payload: Dict object with the kwargs
        @raises: AssertionError and ValueError.
        """
        error_msg = '[devo_senders] validate_kwargs_for_get_next_sender() ->'

        # queue_usage_percentage
        assert 'queue_usage_percentage' in payload, f'{error_msg} The <queue_usage_percentage> argument is mandatory.'
        usage = payload['queue_usage_percentage']
        assert isinstance(usage, float), f'{error_msg} The <queue_usage_percentage> must be an instance of ' \
                                         f'float type not <{type(payload["queue_usage_percentage"])}>'

    def check_if_senders_should_be_disabled(self):
        """ Method that call to the internal method of the Sender to check if it should be disabled. """
        for sender in self._senders:
            sender: DevoSender
            if sender:
                sender.check_if_sender_should_be_disabled()
