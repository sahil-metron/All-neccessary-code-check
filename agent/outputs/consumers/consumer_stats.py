class ConsumerStats:

    def __init__(self):
        self.__consumed_message_counter: int = 0
        self.__consumed_message_size_in_bytes: int = 0

    def increase_message_counter(self,
                                 number: int = 1,
                                 size_in_bytes: int = 0):
        """

        :param number:
        :param size_in_bytes:
        :return:
        """

        self.__consumed_message_counter += number
        self.__consumed_message_size_in_bytes += size_in_bytes

    def get_stats(self) -> (int, int):
        """

        :return:
        """

        temp_counter = self.__consumed_message_counter
        temp_size_in_bytes = self.__consumed_message_size_in_bytes
        self.__consumed_message_counter = 0
        self.__consumed_message_size_in_bytes = 0
        return temp_counter, temp_size_in_bytes
