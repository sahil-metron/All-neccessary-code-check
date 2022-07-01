import logging
import math
import threading
from datetime import datetime
from queue import Queue, Empty
from time import time
from typing import Union, List, Optional

from agent.message.message import Message
from agent.queues.content.collector_queue_item import CollectorQueueItem
from agent.message.lookup_job_factory import LookupJob

# GLOBALS
log = logging.getLogger(__name__)
MessageTypes = Union[Message, List[Message], LookupJob]


class CollectorMultithreadingQueue(Queue):
    """ Devo main internal queue for Multithreading architecture """

    def __init__(self,
                 max_size_in_mb: int = None,
                 max_size_in_messages: int = 0,
                 max_elapsed_time_in_sec: int = 0,
                 max_wrap_size_in_messages: int = 100,
                 generate_metrics: bool = False,
                 generate_collector_details: bool = False,
                 queue_name: str = None,
                 unlimited_privileges: bool = False):
        """Queue builder.

        :param max_size_in_mb: Hard limit in MB of the queue.
        :param max_size_in_messages: Hard limit in number of items of the queue.
        :param max_elapsed_time_in_sec: Used as bottle-neck and timeout detection.
        :param max_wrap_size_in_messages: Max items in a wrap.
        :param generate_collector_details: Define the collector details level.
        :param queue_name: Custom name for the queue.
        :param unlimited_privileges: True if this queue should skip the max size validations. False if not.
        """
        super(CollectorMultithreadingQueue, self).__init__(maxsize=math.ceil(max_size_in_messages * 1.5))

        if queue_name is None:
            queue_name = 'internal_queue'
        queue_suffix = '_multithreading'
        self.queue_name = f"{queue_name}{queue_suffix}"

        assertion_header = '[INTERNAL] CollectorMultiprocessingQueue::__init__() -> <max_size_in_mb>'
        assert isinstance(max_size_in_mb, int), f'{assertion_header} must be an int instance.'
        assert max_size_in_mb > 0, f'{assertion_header} has an invalid value. Valid range: 1-1024.'
        assert max_size_in_mb <= 1024, f'{assertion_header} has an invalid value. Valid range: 1-1024.'

        assertion_header = '[INTERNAL] CollectorMultiprocessingQueue::__init__() -> <max_size_in_items>'
        assert isinstance(max_size_in_messages, int), f'{assertion_header} must be an int instance.'
        assert max_size_in_messages > 0, f'{assertion_header} has an invalid value. Valid range: 1-100000.'
        assert max_size_in_messages <= 100000, f'{assertion_header} has an invalid value. Valid range: 1-100000.'

        assertion_header = '[INTERNAL] CollectorMultiprocessingQueue::__init__() -> <max_elapsed_time_in_sec>'
        assert isinstance(max_elapsed_time_in_sec, int), f'{assertion_header} must be an int instance.'
        assert max_elapsed_time_in_sec > 0, f'{assertion_header} has an invalid value. Valid range: 1-600.'
        assert max_elapsed_time_in_sec <= 1024, f'{assertion_header} has an invalid value. Valid range: 1-600.'

        assertion_header = '[INTERNAL] CollectorMultiprocessingQueue::__init__() -> <max_wrap_size_in_items>'
        assert isinstance(max_wrap_size_in_messages, int), f'{assertion_header} must be an int instance.'
        assert max_wrap_size_in_messages > 0, f'{assertion_header} has an invalid value. Valid range: 1-500.'
        assert max_wrap_size_in_messages <= 500, f'{assertion_header} has an invalid value. Valid range: 1-500.'

        assertion_header = '[INTERNAL] CollectorMultiprocessingQueue::__init__() -> <max_size_in_messages>'
        assert max_size_in_messages > max_wrap_size_in_messages, f'{assertion_header} ' \
                                                                 f'cannot be higher than max_wrap_size_in_messages.'
        # Collector properties
        self.max_size_in_mb: int = max_size_in_mb
        self.max_size_in_bytes: int = (max_size_in_mb * 1024)
        self.max_size_in_messages: int = max_size_in_messages
        self.max_batch_size: int = max_wrap_size_in_messages
        self.max_elapsed_time_in_sec: int = max_elapsed_time_in_sec
        self.__generate_metrics: bool = generate_metrics
        self.__generate_collector_details: bool = generate_collector_details

        # Size counters
        self.queue_size_in_messages: int = 0
        self.queue_size_in_bytes: int = 0
        self.queue_size_lock: threading.Lock = threading.Lock()

        # Lockers and events
        self.sleep_event: threading.Event = threading.Event()
        self.put_lock: threading.Lock = threading.Lock()
        self.elapsed_seconds_putting: int = 0
        self.get_lock: threading.Lock = threading.Lock()
        self.elapsed_seconds_getting: int = 0
        self.__input_lock: threading.Event = threading.Event()

        # Privileges
        self.unlimited_privileges: bool = unlimited_privileges

        # Set the proper "input lock" status for not to be activated by default
        self.__input_lock.set()

    def put(self, item: MessageTypes, block=True, timeout=None):
        """Blocks the <put> action to add new items to the queue after checking the queue maxsize.

        :param item: Event to be queued
        :param block: Defines if the queue must be blocked
        :param timeout: Defines the timeout.
        """
        assertion_header = '[INTERNAL] CollectorMultiprocessingQueue::put() -> <item>'
        assert isinstance(item, (Message, List, LookupJob)), f'{assertion_header} instance type is invalid.'
        assert item is not None, f'{assertion_header} cannot be NULL'

        # Initiate control vars
        start_time_local: Optional[datetime] = datetime.utcnow() if self.__generate_collector_details else None
        has_been_waiting = False
        queue_size_snapshot = None
        internal_queue_size_snapshot = None
        next_queue_size_snapshot = None

        self.__input_lock.wait()

        # Execute the put action
        with self.put_lock:
            if self.__generate_collector_details:
                elapsed_seconds_before_put_lock = (datetime.utcnow() - start_time_local).total_seconds()
                start_time_local = datetime.utcnow()

            queue_items: List[CollectorQueueItem] = self.__wrap_item(item)

            for queue_item in queue_items:
                has_been_waiting = False

                number_of_messages = queue_item.number_of_messages
                size_in_bytes = queue_item.size_in_bytes

                queue_size_snapshot = self.queue_size_in_messages
                internal_queue_size_snapshot = self._qsize()
                next_queue_size = queue_size_snapshot + number_of_messages
                next_queue_size_snapshot = next_queue_size

                # if next_queue_size > self.max_size_in_messages:
                #     has_been_waiting = True
                #     super().join()
                while not self.__check_free_space(queue_item):
                    has_been_waiting = True
                    self.sleep_event.wait()
                    if self.sleep_event.is_set():
                        self.sleep_event.clear()

                if self.__generate_metrics:
                    queue_item.enqueued(self.queue_name)

                super().put(queue_item, block, timeout)
                with self.queue_size_lock:
                    self.queue_size_in_messages += number_of_messages
                    self.queue_size_in_bytes += size_in_bytes

        if self.__generate_collector_details:
            elapsed_seconds_putting = (datetime.utcnow() - start_time_local).total_seconds()

            if log.isEnabledFor(logging.DEBUG) and (
                    has_been_waiting is True or elapsed_seconds_before_put_lock > 1 or elapsed_seconds_putting > 1):
                log_message = \
                    f"[PUT][{self.queue_name}] " \
                    f"Elapsed seconds (before_lock/putting): " \
                    f"{elapsed_seconds_before_put_lock:0.3f}/" \
                    f"{elapsed_seconds_putting:0.3f}, " \
                    f"queue (size/sizeMB/messages/next_messages/maxsize/maxsizeMB): " \
                    f"{internal_queue_size_snapshot}/" \
                    f"{round(self.queue_size_in_bytes / 1024 / 1024, 2)}MB/" \
                    f"{queue_size_snapshot}/" \
                    f"{next_queue_size_snapshot}/" \
                    f"{self.max_size_in_messages}/" \
                    f"{round(self.max_size_in_bytes / 1024 / 1024, 2)}MB, " \
                    f"has_been_waiting: {has_been_waiting}"
                log.debug(log_message)

    def block_input_queue(self):
        """

        :return:
        """

        log_message = \
            f'Blocking the input queue for avoiding the receiving of any new incoming message'
        log.info(log_message)
        self.__input_lock.set()

    def release_input_queue(self):
        """

        :return:
        """

        log_message = \
            f'Releasing the input blocker for continue receiving incoming messages'
        log.info(log_message)
        self.__input_lock.clear()

    def __check_free_space(self, queue_item: CollectorQueueItem) -> bool:
        """Checks if the queue has enough free space taking in consideration the hard limits (items/mem_size).

        :param queue_item: CollectorQueueItem
        :return: True if the queue has enough space. False if not.
        """

        # Some queues have privileges!
        if self.unlimited_privileges is True:
            return True

        # Calculate
        new_size = self.queue_size_in_messages + queue_item.number_of_messages
        new_mem_size = self.queue_size_in_bytes + queue_item.size_in_bytes

        # Check
        size_validation = True if new_size <= self.max_size_in_messages else False
        mem_validation = True if new_mem_size <= self.max_size_in_bytes else False

        # Result
        if size_validation and mem_validation:
            return True

        return False

    def get(self, block: bool = True, timeout: int = None) -> CollectorQueueItem:
        """Gets new items from the queue and decrease the messages and bytes counter.

        :param timeout: Defines a timeout for this action.
        :param block: Defines if the queue must be blocked
        :return: An item that can be a MessageBath or str instance.
        """

        start_time_local: Optional[datetime] = None
        if self.__generate_collector_details:
            start_time_local = datetime.utcnow()

        with self.get_lock:
            if self.__generate_collector_details:
                elapsed_seconds_before_get_lock = (datetime.utcnow() - start_time_local).total_seconds()
                start_time_local = datetime.utcnow()

            # Get queue item and release the put event.waiter
            queue_item: CollectorQueueItem = self.__internal_get(block=block, timeout=timeout)
            self.sleep_event.set()

            number_of_messages = queue_item.number_of_messages
            size_in_bytes = queue_item.size_in_bytes

            with self.queue_size_lock:
                self.queue_size_in_messages -= number_of_messages
                self.queue_size_in_bytes -= size_in_bytes

            if self.__generate_collector_details:
                elapsed_seconds_getting = (datetime.utcnow() - start_time_local).total_seconds()

                if log.isEnabledFor(logging.DEBUG) and (
                        elapsed_seconds_before_get_lock > 1 or elapsed_seconds_getting > 1):
                    log_message = \
                        f"[GET][{self.queue_name}] " \
                        f"Elapsed seconds (before_lock/getting): " \
                        f"{elapsed_seconds_before_get_lock:0.3f}/" \
                        f"{elapsed_seconds_getting:0.3f}, " \
                        f"number_of_messages: {number_of_messages}, " \
                        f"size_in_bytes: {size_in_bytes}, " \
                        f"queue_size: {self.qsize()}"
                    log.debug(log_message)

            if self.__generate_metrics:
                queue_item.dequeued(self.queue_name)

            return queue_item

    def __internal_get(self, block=True, timeout=None):
        """Remove and return an item from the queue.

        If optional args 'block' is true and 'timeout' is None (the default),
        block if necessary until an item is available. If 'timeout' is
        a non-negative number, it blocks at most 'timeout' seconds and raises
        the Empty exception if no item was available within that time.
        Otherwise ('block' is false), return an item if one is immediately
        available, else raise the Empty exception ('timeout' is ignored
        in that case).
        """
        with self.not_empty:
            start_time_local = datetime.utcnow()
            has_been_waiting = False
            if not block:
                if not self._qsize():
                    raise Empty
            elif timeout is None:
                while not self._qsize():
                    has_been_waiting = True
                    self.not_empty.wait()
            elif timeout < 0:
                raise ValueError("'timeout' must be a non-negative number")
            else:
                end_time = time() + timeout
                while not self._qsize():
                    has_been_waiting = True
                    remaining = end_time - time()
                    if remaining <= 0.0:
                        raise Empty
                    self.not_empty.wait(remaining)
            item = self._get()
            elapsed_seconds_getting = (datetime.utcnow() - start_time_local).total_seconds()
            if has_been_waiting is True and self._qsize() > 0 and elapsed_seconds_getting > 1:
                log_message = \
                    f"[GET][{self.queue_name}] " \
                    f"elapsed_seconds_internal_waiting: {elapsed_seconds_getting:0.6f}, " \
                    f"size: {self._qsize()}"
                log.debug(log_message)
            self.not_full.notify()
            return item

    def qsize(self) -> int:
        return self.queue_size_in_messages

    def get_maxsize(self) -> int:
        return self.max_size_in_messages

    def is_generating_metrics(self):
        return self.__generate_metrics

    def __wrap_item(self, item: Union[Message, List[Message], LookupJob]) -> List[CollectorQueueItem]:
        if isinstance(item, (Message, LookupJob)):
            return [CollectorQueueItem(item)]
        elif isinstance(item, list):
            number_of_slides = math.ceil(len(item) / self.max_batch_size)
            message_list = []
            for i in range(number_of_slides):
                slide_batch = item[i * self.max_batch_size:(i + 1) * self.max_batch_size]
                message_list.append(CollectorQueueItem(slide_batch))
            return message_list
        else:
            raise TypeError("Unexpected item type")

    def __repr__(self) -> str:
        """ String representation of the queue """
        str_representation: str = \
            f'({self.__class__.__name__}) {self.queue_name} -> ' \
            f'max_size_in_messages: {self.max_size_in_messages}, ' \
            f'max_size_in_mb: {self.max_size_in_mb}, ' \
            f'max_wrap_size_in_items: {self.max_batch_size}'

        return str_representation
