from agent.message.output_object import OutputObject


class MessageBatch(OutputObject):

    def __init__(
            self,
            message_list: list):
        super().__init__("standard_message_batch")
        if not isinstance(message_list, list):
            raise Exception("Parameter \"message_list\" must be a \"list\"")

        self._messages: list = message_list

    @property
    def messages(self) -> list:
        return self._messages

    @property
    def size(self) -> int:
        return len(self._messages) if self._messages else 0

    def __str__(self) -> str:
        return f"number_of_messages: \"{self.size}\""
