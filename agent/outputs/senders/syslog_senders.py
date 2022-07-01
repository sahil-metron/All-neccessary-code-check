from agent.outputs.senders.abstracts.senders_abstract import SendersAbstract, SenderType
from agent.outputs.senders.syslog_sender import SyslogSender


class SyslogSenders(SendersAbstract):
    """ Senders Abstraction for Syslog """

    def get_next_sender(self, **kwargs) -> SenderType:
        """
        Method that returns the next sender to be used by the manager.
        @param kwargs: Arguments compatibility with all abstractions.
        @return: An instance of a sender.
        @raise: An ValueError when there is no sender available.
        """
        if len(self._senders) > 0:

            # Get the next sender
            sender: SyslogSender = self._senders[self._next_sender_index]

            # Update the next sender id
            self._next_sender_index += 1
            if self._next_sender_index == len(self._senders):
                self._next_sender_index = 0

            return sender

        raise ValueError('[SyslogSenders] get_next_sender(method) -> No senders instance available')
