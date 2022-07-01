import logging
import math
import time
from collections import deque
from datetime import datetime
from statistics import mean
from threading import Thread
from typing import Union

from agent.commons.collector_utils import CollectorUtils
from agent.outputs.candidate_to_be_removed.output_consumer import OutputConsumer
from agent.outputs.candidate_to_be_removed.output_senders import OutputSenders
from agent.queues.collector_multiprocessing_queue import CollectorMultiprocessingQueue
from agent.queues.collector_multithreading_queue import CollectorMultithreadingQueue

log = logging.getLogger(__name__)

QueueType = Union[CollectorMultiprocessingQueue, CollectorMultithreadingQueue]


class OutputConsumers(Thread):

    def __init__(self,
                 group_name: str,
                 output_queue: QueueType,
                 output_senders: OutputSenders,
                 max_consumer_number: int,
                 generate_collector_details: bool):
        super().__init__()
        self.name = \
            self.__class__.__name__ + f"({group_name},{CollectorUtils.collector_name})"
        self._output_consumers: list = []
        self.group_name = group_name
        self.output_queue: QueueType = output_queue
        self.output_senders: OutputSenders = output_senders
        self.queue_size_stats: deque = deque(maxlen=6)
        self.number_of_consumer_stats: deque = deque(maxlen=10)
        self.max_consumer_number = max_consumer_number
        self.__generate_collector_details: bool = generate_collector_details

        self.same_consumer_number_counter: int = 0
        self.running_flag: bool = True

    def _decrease_consumers(self, amount: int = 1):
        for _ in range(amount):
            consumer: OutputConsumer = self._output_consumers.pop()
            consumer.stop()

    def _increase_consumers(self, amount: int = 1):
        for _ in range(amount):
            next_consumer_id: int = len(self._output_consumers) + 1
            consumer: OutputConsumer = \
                OutputConsumer(
                    self.group_name,
                    next_consumer_id,
                    self.output_queue,
                    self.output_senders,
                    self.__generate_collector_details
                )
            self._output_consumers.append(consumer)
            consumer.start()

    def adjust_consumer_number(self):
        max_size_of_queue = self.output_queue.get_maxsize()
        queue_size = self.output_queue.qsize()
        previous_number_of_consumer = self.number_of_consumer_stats[-1] if self.number_of_consumer_stats else 0
        self.queue_size_stats.append(queue_size)
        max_queue_size_value = max(self.queue_size_stats)
        number_of_consumer_from_avg: bool = False

        if max_queue_size_value > 0:
            usage_percentage = max_queue_size_value / max_size_of_queue
            number_of_consumers = round(self.max_consumer_number * usage_percentage)
        else:
            number_of_consumers = 1

        if previous_number_of_consumer > number_of_consumers:
            number_of_consumers_avg = math.ceil(mean(self.number_of_consumer_stats))
            if number_of_consumers_avg > number_of_consumers:
                number_of_consumers = number_of_consumers_avg
                number_of_consumer_from_avg = True
                for _ in range(self.number_of_consumer_stats.maxlen):
                    self.number_of_consumer_stats.append(number_of_consumers_avg)

        if previous_number_of_consumer == number_of_consumers:
            self.same_consumer_number_counter += 1
            if self.same_consumer_number_counter >= 10000:
                self.same_consumer_number_counter = 10
                log.warning(
                    f"The counter has reached a large value and there is no need to continue counting, "
                    f"it will be reset to a lower value that will no impact with the main functionality "
                    f"(new value: {self.same_consumer_number_counter})"
                )

        else:
            self.same_consumer_number_counter = 0

        if self.same_consumer_number_counter >= 7 and number_of_consumer_from_avg:
            number_of_consumers_reduced = round(mean(self.number_of_consumer_stats) * 0.8)
            if number_of_consumers_reduced > 1:
                for _ in range(self.number_of_consumer_stats.maxlen):
                    self.number_of_consumer_stats.append(number_of_consumers_reduced)
                log.debug(
                    f"Forcing the reduction of number of consumers: "
                    f"{number_of_consumers} > {number_of_consumers_reduced}"
                )
                number_of_consumers = number_of_consumers_reduced
            else:
                log.debug(
                    f"Previous consumer number was having the minimum value, "
                    f"nothing will be done, number_of_consumers: {number_of_consumers}"
                )
            self.same_consumer_number_counter = 0

        if (len(self.number_of_consumer_stats) > 0 and number_of_consumers != self.number_of_consumer_stats[-1]) \
                or len(self.number_of_consumer_stats) == 0:
            self.number_of_consumer_stats.append(number_of_consumers)

        self._set_number_of_consumers(number_of_consumers)

    def _set_number_of_consumers(self, number_of_consumers: int):
        consumers_size = len(self._output_consumers)
        if consumers_size > number_of_consumers:
            self._decrease_consumers(consumers_size - number_of_consumers)
        elif consumers_size < number_of_consumers:
            self._increase_consumers(number_of_consumers - consumers_size)

    def get_amount(self) -> int:
        return len(self._output_consumers)

    def __str__(self):
        return f"{{'consumers': {self._output_consumers}, senders: {self.output_senders}"

    def get_status(self):
        """

        :return:
        """

        number_of_consumer_stats_avg = \
            math.ceil(mean(self.number_of_consumer_stats)) if self.number_of_consumer_stats else 0
        return f"running_consumers: {len(self._output_consumers)}, " \
               f"same_consumer_number_counter: {self.same_consumer_number_counter}, " \
               f"queue_size_stats: {list(self.queue_size_stats)}, " \
               f"number_of_consumer_stats: {list(self.number_of_consumer_stats)}, " \
               f"number_of_consumer_stats_avg: {number_of_consumer_stats_avg}"

    def stop(self):
        """

        :return:
        """

        log.info(f"Destroying all available consumers")
        self._set_number_of_consumers(0)
        self.running_flag = False

    def run(self):
        """

        :return:
        """

        last_timestamp = datetime.utcnow()
        show_consumer_status_every_in_seconds = 300
        while self.running_flag:
            self.adjust_consumer_number()
            now = datetime.utcnow()
            if (now-last_timestamp).total_seconds() >= show_consumer_status_every_in_seconds:
                log.info(f"{self.get_status()}")
                last_timestamp = now
            else:
                log.debug(f"{self.get_status()}")
            time.sleep(10)
        log.info(f"Finalizing thread")
        log.debug(f"output_consumers: {self._output_consumers}")

    def start(self):
        log.info(f"[OUTPUT] {self.getName()} -> Starting thread")
        self._set_number_of_consumers(1)
        log.debug(f"Number of consumers: {len(self._output_consumers)}")
        super().start()
