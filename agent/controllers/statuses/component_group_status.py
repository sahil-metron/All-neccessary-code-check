import json

from agent.controllers.statuses.component_status import ComponentStatus


class ComponentGroupStatus:

    def __init__(self, name: str = None):
        """

        :param name:
        """
        if name is None:
            raise Exception('Property "name" is mandatory')

        self.__name: str = name
        self.__components: dict = {}

    @property
    def name(self) -> str:
        """

        :return:
        """
        return self.__name

    @property
    def components(self) -> dict:
        """

        :return:
        """
        return self.__components

    def add_component_to_group(self, component_status: ComponentStatus):
        """

        :param component_status:
        :return:
        """
        if component_status.name in self.__components.keys():
            raise Exception(
                f'Component "{component_status.name}" already exists in group "{self.__name}"'
            )
        self.__components[component_status.name] = component_status

    def get_component_status(self, component_name: str) -> ComponentStatus:
        """

        :param component_name:
        :return:
        """
        return self.__components.get(component_name)

    def __str__(self) -> str:
        status_dict: dict = {
            "name": self.__name,
            "number_of_components": len(self.__components)
        }
        return json.dumps(status_dict)

    def __repr__(self):
        return self.__str__()
