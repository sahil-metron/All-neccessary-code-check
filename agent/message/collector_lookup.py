from queue import Queue

from agent.message.output_object import OutputObject


class CollectorLookup(OutputObject):

    def __init__(self, name: str, headers: list = None, action_type: str = None, control_delay_in_seconds=5):
        super().__init__("dynamic_lookup")
        if headers is None:
            headers = []
        if action_type is None or (action_type != "INC" and action_type != "FULL"):
            raise Exception("\"action_type\" parameter must wrong value, valid values: (\"INC\"|\"FULL\")")
        self._name = name
        self._action_type = action_type
        self._headers: list = headers
        self._data_groups: Queue = Queue()
        self.executed_start: bool = False
        self.execute_end: bool = False
        self.executed_end: bool = False
        self.control_delay_in_seconds: int = control_delay_in_seconds

    @property
    def name(self) -> str:
        return self._name

    @property
    def action_type(self) -> str:
        return self._action_type

    @property
    def headers(self) -> list:
        return self._headers

    def add_data_group(self, data: list, is_delete: bool = False):
        self._data_groups.put(
            {
                "data": data,
                "is_delete": is_delete
            }
        )

    def get_next_data_group(self) -> (list, bool):
        data_group = self._data_groups.get_nowait()
        if data_group:
            return data_group["data"], data_group["is_delete"]
        return None, False

    def is_empty(self) -> bool:
        return self._data_groups.empty()
