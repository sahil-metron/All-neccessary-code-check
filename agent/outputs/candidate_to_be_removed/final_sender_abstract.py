from threading import Thread


class UniqueSenderAbstract(Thread):

    def __init__(self, group_name: str, instance_name: str, configuration: dict):
        super().__init__()
        self.group_name = group_name
        self.instance_name = instance_name
        self.configuration = configuration

