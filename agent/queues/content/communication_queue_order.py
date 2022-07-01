import json
from typing import Union


class CommunicationQueueOrder:
    """
    This class will represent an order
    """

    INPUT_TYPE: str = "input"
    OUTPUT_TYPE: str = "output"
    GLOBAL_TYPE: str = "global"

    def __init__(self,
                 order_id: str = None,
                 details: str = None,
                 order_type: str = None):
        """

        :param order_id:
        :param details:
        :param order_type:
        """

        self.__order_id: str = order_id
        if order_type is None:
            order_type = CommunicationQueueOrder.GLOBAL_TYPE
        self.__order_type: str = order_type
        self.__details: str = details

    @property
    def id(self) -> str:
        """

        :return:
        """
        return self.__order_id

    @property
    def details(self) -> str:
        """

        :return:
        """
        return self.__details

    @property
    def type(self) -> str:
        """

        :return:
        """
        return self.__order_type

    def is_output(self) -> bool:
        """

        :return:
        """
        return self.__order_type == CommunicationQueueOrder.OUTPUT_TYPE

    def is_input(self) -> bool:
        """

        :return:
        """
        return self.__order_type == CommunicationQueueOrder.INPUT_TYPE

    def __str__(self) -> str:
        notification_dict: dict = {
            "order_id": self.__order_id,
            "order_type": self.__order_type,
            "details": self.__details
        }
        return json.dumps(notification_dict)

    def __repr__(self):
        return self.__str__()

    def __eq__(self, other: Union['CommunicationQueueOrder', str]) -> bool:
        if isinstance(other, str):
            return self.__order_id == other
        return self.__order_id == other.__order_id
