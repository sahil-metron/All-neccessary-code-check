class OutputObject:

    def __init__(self, output_object_type: str) -> None:
        self.__output_object_type = output_object_type
        if self.__output_object_type is None:
            self.__output_object_type = "unknown"

    def __del__(self):
        self.__output_object_type = None

    @property
    def output_object_type(self):
        return self.__output_object_type
