import copy
import threading
from datetime import datetime
from typing import Union

from agent.outputs.senders.stats.sender_message_stats import SenderMessageStats

SenderType = Union['SyslogSender', 'ConsoleSender', 'DevoSender']


class SenderStats(object):

    def __init__(self, sender_instance: SenderType):
        self.sender_instance: SenderType = sender_instance

        self.stats_values: SenderMessageStats = SenderMessageStats(self.sender_instance.getName())
        self.stats_values_lock: threading.Lock = threading.Lock()

    def increase_standard_messages_counter(self, elapsed_time_in_seconds: float):
        with self.stats_values_lock:
            self.stats_values.increase_standard_messages_counter(elapsed_time_in_seconds)

    def increase_internal_messages_counter(self, elapsed_time_in_seconds: float):
        with self.stats_values_lock:
            self.stats_values.increase_internal_messages_counter(elapsed_time_in_seconds)

    def increase_lookup_counter(self, elapsed_time_in_seconds: float):
        with self.stats_values_lock:
            self.stats_values.increase_lookup_counter(elapsed_time_in_seconds)

    def mark_as_shown(self, timestamp: datetime):
        with self.stats_values_lock:
            self.stats_values.mark_as_shown(timestamp)

    def get_stats_snapshot(self, timestamp: datetime) -> SenderMessageStats:
        with self.stats_values_lock:
            self.stats_values.set_status(self.sender_instance.get_status())
            stats_snapshot = copy.deepcopy(self.stats_values)
            self.stats_values.mark_as_shown(timestamp)
            return stats_snapshot

    def add_enqueued_elapsed_times(self, enqueued_elapsed_times: dict):
        with self.stats_values_lock:
            self.stats_values.add_enqueued_elapsed_times(enqueued_elapsed_times)
