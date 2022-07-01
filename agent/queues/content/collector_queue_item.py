import json
from datetime import datetime
from typing import Union, List

from agent.message.collector_lookup import CollectorLookup
from agent.message.message import Message
from agent.message.lookup_job_factory import LookupJob

MESSAGE_STANDARD = "standard"
MESSAGE_BATCH = "batch"
MESSAGE_LOOKUP = "lookup"
MESSAGE_LOOKUP_JOB = "lookup_job"
MESSAGE_UNKNOWN = "unknown"


class CollectorQueueItem:

    def __init__(self, content: Union[Message, List[Message], CollectorLookup]):
        self.__content: Union[Message, List[Message], CollectorLookup] = content
        self.__type: str = MESSAGE_UNKNOWN
        self.__number_of_messages: int = 0
        self.__size_in_bytes: int = 0
        self.__in_queue_timestamps: dict = {}

        if isinstance(content, list):
            self.__type = MESSAGE_BATCH
            self.__number_of_messages = len(content)
            self.__size_in_bytes: int = content.__sizeof__()
        elif isinstance(content, Message):
            self.__type = MESSAGE_STANDARD
            self.__number_of_messages = 1
            self.__size_in_bytes: int = content.__sizeof__()
        elif isinstance(content, CollectorLookup):
            self.__type = MESSAGE_LOOKUP
            self.__number_of_messages = 1
            self.__size_in_bytes: int = content.__sizeof__()
        elif isinstance(content, LookupJob):
            self.__type = MESSAGE_LOOKUP_JOB
            self.__number_of_messages = len(content.to_do)
            self.__size_in_bytes: int = content.__sizeof__()

    def __del__(self):
        if self.__type == MESSAGE_BATCH:
            self.__content.clear()
        self.__content = None
        self.__size_in_bytes = 0

    @property
    def number_of_messages(self) -> int:
        return self.__number_of_messages

    @property
    def size_in_bytes(self) -> int:
        return self.__size_in_bytes

    @property
    def type(self) -> str:
        return self.__type

    @property
    def content(self):
        return self.__content

    def __str__(self):
        queue_item_dict = {
                "type": self.type,
                "number_of_messages": self.number_of_messages,
                "size_in_bytes": self.size_in_bytes,
                "content": str(self.content)
            }
        return json.dumps(queue_item_dict)

    def enqueued(self, queue_name: str):
        self.__in_queue_timestamps[queue_name] = {"in": datetime.utcnow()}

    def dequeued(self, queue_name: str):
        self.__in_queue_timestamps[queue_name]["out"] = datetime.utcnow()

    @property
    def enqueued_times(self) -> dict:
        return self.__in_queue_timestamps

    def elapsed_times(self) -> dict:
        elapsed_times = {}
        for queue_name, queue_stats in self.__in_queue_timestamps.items():
            elapsed_times[queue_name] = (queue_stats["out"] - queue_stats["in"]).total_seconds()
        return elapsed_times
