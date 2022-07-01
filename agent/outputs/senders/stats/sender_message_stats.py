from datetime import datetime

import pytz


class SenderMessageStats(object):

    def __init__(self, sender_name: str):
        self._sender_name: str = sender_name
        self._sender_status: str = "UNKNOWN"
        self._last_timestamp: datetime = datetime.utcnow().replace(tzinfo=pytz.utc)

        self._number_of_messages_sent_total: int = 0
        self._number_of_messages_sent_partial: int = 0
        self._elapsed_time_messages_sent_partial_in_seconds: int = 0

        self._number_of_standard_messages_sent_total: int = 0
        self._number_of_standard_messages_sent_partial: int = 0
        self._elapsed_time_standard_messages_sent_partial_in_seconds: int = 0

        self._number_of_internal_messages_sent_total: int = 0
        self._number_of_internal_messages_sent_partial: int = 0
        self._elapsed_time_internal_messages_sent_partial_in_seconds: int = 0

        self._number_of_lookup_messages_sent_total: int = 0
        self._number_of_lookup_messages_sent_partial: int = 0
        self._elapsed_time_lookup_messages_sent_partial_in_seconds: int = 0

        self.__enqueued_elapsed_times_in_seconds_stats: dict = {}

    def add_enqueued_elapsed_times(self, enqueued_elapsed_times_in_seconds: dict):

        for queue_name, enqueued_elapsed_time_in_seconds in enqueued_elapsed_times_in_seconds.items():
            if queue_name not in self.__enqueued_elapsed_times_in_seconds_stats:
                self.__enqueued_elapsed_times_in_seconds_stats[queue_name] = {"max": -1}
            queue_stats = self.__enqueued_elapsed_times_in_seconds_stats[queue_name]

            # Update of avg value
            if "avg" not in queue_stats:
                queue_stats["avg"] = enqueued_elapsed_time_in_seconds
            else:
                queue_stats["avg"] = (queue_stats["avg"] + enqueued_elapsed_time_in_seconds) / 2

            # Update of maximum value
            if enqueued_elapsed_time_in_seconds > queue_stats["max"]:
                queue_stats["max"] = enqueued_elapsed_time_in_seconds

    def _increase_global_message_counter(self, elapsed_time_in_seconds: float):
        self._number_of_messages_sent_total += 1
        self._number_of_messages_sent_partial += 1
        self._elapsed_time_messages_sent_partial_in_seconds += elapsed_time_in_seconds

    def increase_standard_messages_counter(self, elapsed_time_in_seconds: float):
        self._number_of_standard_messages_sent_total += 1
        self._number_of_standard_messages_sent_partial += 1
        self._elapsed_time_standard_messages_sent_partial_in_seconds += elapsed_time_in_seconds
        self._increase_global_message_counter(elapsed_time_in_seconds)

    def increase_internal_messages_counter(self, elapsed_time_in_seconds: float):
        self._number_of_internal_messages_sent_total += 1
        self._number_of_internal_messages_sent_partial += 1
        self._elapsed_time_internal_messages_sent_partial_in_seconds += elapsed_time_in_seconds
        self._increase_global_message_counter(elapsed_time_in_seconds)

    def increase_lookup_counter(self, elapsed_time_in_seconds: float):
        self._number_of_lookup_messages_sent_total += 1
        self._number_of_lookup_messages_sent_partial += 1
        self._elapsed_time_lookup_messages_sent_partial_in_seconds += elapsed_time_in_seconds
        self._increase_global_message_counter(elapsed_time_in_seconds)

    def get_queue_stats(self):
        return \
            f'enqueued_elapsed_times_in_seconds_stats: {self.__enqueued_elapsed_times_in_seconds_stats}'

    def get_global_stats(self) -> str:
        return \
            f'Sender: {self._sender_name}, ' \
            f'status: {self._sender_status}'

    def get_global_messages_stats(self) -> str:
        return \
            f'Global - Total number of messages sent: {self._number_of_messages_sent_total}, ' \
            f'messages sent since "{self._last_timestamp}": {self._number_of_messages_sent_partial} ' \
            f'(elapsed {self._elapsed_time_messages_sent_partial_in_seconds:0.3f} seconds)'

    def get_internal_messages_stats(self) -> str:
        return \
            f'Internal - Total number of messages sent: {self._number_of_internal_messages_sent_total}, ' \
            f'messages sent since "{self._last_timestamp}": {self._number_of_internal_messages_sent_partial} ' \
            f'(elapsed {self._elapsed_time_internal_messages_sent_partial_in_seconds:0.3f} seconds)'

    def get_standard_messages_stats(self) -> str:
        return \
            f'Standard - Total number of messages sent: {self._number_of_standard_messages_sent_total}, ' \
            f'messages sent since "{self._last_timestamp}": {self._number_of_standard_messages_sent_partial} ' \
            f'(elapsed {self._elapsed_time_standard_messages_sent_partial_in_seconds:0.3f} seconds)'

    def get_lookup_messages_stats(self) -> str:
        return \
            f'Lookup - Total number of messages sent: {self._number_of_lookup_messages_sent_total}, ' \
            f'messages sent since "{self._last_timestamp}": {self._number_of_lookup_messages_sent_partial} ' \
            f'(elapsed {self._elapsed_time_lookup_messages_sent_partial_in_seconds:0.3f} seconds)'

    def mark_as_shown(self, timestamp: datetime):
        self._last_timestamp: datetime = timestamp

        self._number_of_messages_sent_partial: int = 0
        self._elapsed_time_messages_sent_partial_in_seconds: int = 0

        self._number_of_standard_messages_sent_partial: int = 0
        self._elapsed_time_standard_messages_sent_partial_in_seconds: int = 0

        self._number_of_internal_messages_sent_partial: int = 0
        self._elapsed_time_internal_messages_sent_partial_in_seconds: int = 0

        self._number_of_lookup_messages_sent_partial: int = 0
        self._elapsed_time_lookup_messages_sent_partial_in_seconds: int = 0

        for queue_stats in self.__enqueued_elapsed_times_in_seconds_stats.values():
            queue_stats["max"] = -1

    def set_status(self, status: str):
        self._sender_status = status
