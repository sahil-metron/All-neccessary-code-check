import json
from typing import Union


class CommunicationQueueNotification:
    """
    This class will represent a notification
    """

    def __init__(self, notification_id: str = None, details: str = None):
        self.__notification_id: str = notification_id
        self.__details: str = details

    @property
    def details(self) -> str:
        """

        :return:
        """
        return self.__details

    def __str__(self) -> str:
        notification_dict: dict = {
            "notification_id": self.__notification_id,
            "details": self.__details
        }
        return json.dumps(notification_dict)

    def __repr__(self):
        return self.__str__()

    def __eq__(self, other: Union['CommunicationQueueNotification', str]) -> bool:
        if isinstance(other, str):
            return self.__notification_id == other
        return self.__notification_id == other.__notification_id
