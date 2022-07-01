import json

from agent.controllers.statuses.status_enum import StatusEnum


class ComponentStatus:

    def __init__(self, name: str, status: StatusEnum = None):
        self.__name = name
        if status is None:
            status = StatusEnum.UNKNOWN
        self.__status = status

    @property
    def name(self) -> str:
        """

        :return:
        """
        return self.__name

    @property
    def status(self) -> StatusEnum:
        """

        :return:
        """
        return self.__status

    @status.setter
    def status(self, status: StatusEnum):
        self.__status = status

    def __str__(self) -> str:
        status_dict: dict = {
            "name": self.__name,
            "status": str(self.__status)
        }
        return json.dumps(status_dict)

    def __repr__(self):
        return self.__str__()
