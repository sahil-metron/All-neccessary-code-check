import hashlib
import json
from datetime import datetime
from typing import Union, Optional

import pytz

from agent.commons.constants import Constants
from agent.message.output_object import OutputObject


class Message(OutputObject):

    TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S.%f"

    def __init__(
            self,
            message_timestamp: Union[datetime, str, int, float],
            message_tag: str,
            message_content: str,
            output_target: Optional[list] = None,
            is_internal: Optional[bool] = None):
        super().__init__("standard_message")

        self.__internal_timestamp: float = datetime.utcnow().timestamp()

        if isinstance(message_timestamp, datetime):
            if message_timestamp.tzinfo is None:
                message_timestamp = message_timestamp.replace(tzinfo=pytz.utc)
            self.__timestamp: str = message_timestamp.astimezone(pytz.utc).strftime(Message.TIMESTAMP_FORMAT)[:23]
        elif isinstance(message_timestamp, str):
            self.__timestamp: str = message_timestamp
        elif isinstance(message_timestamp, int):
            if len(str(message_timestamp)) > 10:
                message_timestamp /= 1e3
            self.__timestamp: str = \
                datetime.fromtimestamp(message_timestamp).astimezone(pytz.utc).strftime(Message.TIMESTAMP_FORMAT)[:23]
        elif isinstance(message_timestamp, float):
            self.__timestamp: str = \
                datetime.fromtimestamp(message_timestamp).astimezone(pytz.utc).strftime(Message.TIMESTAMP_FORMAT)[:23]
        else:
            raise Exception("Parameter \"message_timestamp\" must be a \"datetime\", \"str\" or \"int\"")

        if isinstance(message_tag, str) is False:
            raise Exception("Parameter \"message_tag\" must be a \"str\"")
        message_tag_length = len(message_tag)
        if message_tag_length > 1024:
            raise Exception(
                f"Length of parameter \"message_tag\" is more than "
                f"1024 characters, current size: {message_tag_length}, value: \"{message_tag}\""
            )
        self.__tag: str = message_tag

        self.__output_target = output_target

        if is_internal is None:
            if self.__tag.startswith(Constants.INTERNAL_MESSAGE_TAGGING_BASE):
                self.__is_internal: bool = True
            else:
                self.__is_internal: bool = False
        else:
            self.__is_internal: bool = is_internal

        if isinstance(message_content, str) is False:
            raise Exception("Parameter \"message_content\" must be a \"str\"")
        self.__content: str = message_content

    def __del__(self):
        self.__timestamp = None
        self.__tag = None
        self.__is_internal = None
        self.__content = None
        super().__del__()

    @property
    def timestamp(self) -> str:
        """

        :return:
        """
        return self.__timestamp

    @property
    def tag(self) -> str:
        """

        :return:
        """
        return self.__tag

    @property
    def output_target(self) -> list:
        """

        :return:
        """
        return self.__output_target

    @property
    def content(self) -> str:
        """

        :return:
        """
        return self.__content

    @property
    def is_internal(self) -> bool:
        """

        :return:
        """
        return self.__is_internal

    def __str__(self) -> str:
        message_dict = {
                "timestamp": self.timestamp,
                "tag": self.tag,
                "content": self.content
            }
        return json.dumps(message_dict)

    def __repr__(self):
        return self.__str__()

    def __sizeof__(self):
        return \
            self.output_object_type.__sizeof__() + \
            Message.TIMESTAMP_FORMAT.__sizeof__() + \
            self.__timestamp.__sizeof__() + \
            self.__tag.__sizeof__() + \
            self.__content.__sizeof__() + \
            self.__is_internal.__sizeof__()

    def syslog_size_in_bytes(self) -> int:
        """

        :return:
        """

        # The first 4 bytes are related to facility+severity field (having the value "<14>")
        # and last 4 bytes are for the separators from each message part
        return 4 + len(self.__timestamp) + len(self.__tag) + len(self.__content) + 4

    def message_id(self) -> str:
        """Unique id generated using several message fields

        :return:
        """
        message_id_seed: str = \
            f'{self.__internal_timestamp},' \
            f'{self.__timestamp},' \
            f'{self.__tag},' \
            f'{self.__output_target},' \
            f'{self.__content}'
        h_value = hashlib.sha224(message_id_seed.encode(), usedforsecurity=False).hexdigest()
        return h_value
