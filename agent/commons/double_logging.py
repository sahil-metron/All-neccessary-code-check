import logging
from abc import ABC, abstractmethod
from typing import Union

log = logging.getLogger(__name__)


class DoubleLogging(ABC):

    @abstractmethod
    def send_internal_collector_message(self,
                                        message_content: Union[str, dict],
                                        level: str = None,
                                        shared_domain: bool = False) -> None:
        """

        :param message_content:
        :param level:
        :param shared_domain:
        :return:
        """

        pass

    def log_error(self, message: str) -> None:
        """

        :param message:
        :return:
        """

        if log.isEnabledFor(logging.ERROR):
            log.error(message)
            self.send_internal_collector_message(message, level="error")

    def log_warning(self, message: str) -> None:
        """

        :param message:
        :return:
        """

        if log.isEnabledFor(logging.WARNING):
            log.warning(message)
            self.send_internal_collector_message(message, level="warning")

    def log_info(self, message: str) -> None:
        """

        :param message:
        :return:
        """

        if log.isEnabledFor(logging.INFO):
            log.info(message)
            self.send_internal_collector_message(message, level="info")

    def log_debug(self, message: str) -> None:
        """

        :param message:
        :return:
        """

        if log.isEnabledFor(logging.DEBUG):
            log.debug(message)
            self.send_internal_collector_message(message, level="debug")
