import json
import logging
from datetime import datetime
from queue import Empty
from threading import Thread
from typing import Union, Optional

from agent.outputs.candidate_to_be_removed.output_senders import OutputSenders
from agent.queues.collector_multiprocessing_queue import CollectorMultiprocessingQueue
from agent.queues.collector_multithreading_queue import CollectorMultithreadingQueue
from agent.queues.content.collector_queue_item import CollectorQueueItem

log = logging.getLogger(__name__)

QueueType = Union[CollectorMultiprocessingQueue, CollectorMultithreadingQueue]


class OutputConsumer(Thread):

    def __init__(self,
                 group_name: str,
                 consumer_identification: int,
                 output_queue: QueueType,
                 output_senders: OutputSenders,
                 generate_collector_details: bool):
        super().__init__()
        self.name = f"{group_name}_consumer_{consumer_identification}"
        self.output_queue: QueueType = output_queue
        self.output_senders: OutputSenders = output_senders
        self.__generate_collector_details: bool = generate_collector_details

        self.__running_flag: bool = True
        self.__flush_to_persistence_mode: bool = False

    def run(self) -> None:
        """

        :return:
        """
        start_time_local: Optional[datetime] = None
        while self.__running_flag:
            try:
                queue_item: CollectorQueueItem = self.output_queue.get(timeout=10)
                if queue_item:
                    if self.__generate_collector_details:
                        start_time_local = datetime.utcnow()
                    sender_statuses = self.output_senders.add_output_object(queue_item)

                    # Forcing the deleting object once it has been used
                    del queue_item

                    if self.__generate_collector_details and sender_statuses:
                        elapsed_seconds_adding_to_sender = (datetime.utcnow() - start_time_local).total_seconds()
                        if elapsed_seconds_adding_to_sender > 1:
                            log.debug(
                                f"elapsed_seconds_adding_to_sender: {elapsed_seconds_adding_to_sender}, "
                                f"{sender_statuses}"
                            )
                else:
                    log.error("\"None\" object received")
                self.output_queue.task_done()
            except Empty:
                pass
        log.debug("Finalizing {}".format(self.getName()))

    def start(self):
        """

        :return:
        """
        super().start()

    def stop(self):
        """

        :return:
        """
        self.__running_flag = False

    def enable_flush_to_persistence_system_mode(self):
        """

        :return:
        """
        self.__flush_to_persistence_mode = True

    def __str__(self):
        return json.dumps(
            {
                "name": self.name,
                "running": self.__running_flag,
                "senders": self.output_senders,
                "flush_to_persistence_mode": self.__flush_to_persistence_mode
            }
        )
