import logging
import math
import multiprocessing
import threading
from datetime import datetime
from multiprocessing.queues import JoinableQueue, Queue
from typing import Union, List, Optional

from agent.queues.content.collector_queue_item import CollectorQueueItem
from agent.message.message import Message
from agent.message.lookup_job_factory import LookupJob

# GLOBALS
log = logging.getLogger(__name__)
MessageTypes = Union[Message, List[Message], LookupJob]


# The following implementation of custom MyQueue to avoid NotImplementedError
# when calling JoinableQueue.qsize() in MacOS X comes almost entirely from this github
# discussion: https://github.com/keras-team/autokeras/issues/368
# Necessary modification is made to make the code compatible with Python3.


class SharedCounter(object):
    """ A synchronized shared counter.
    The locking done by multiprocessing.Value ensures that only a single
    process or thread may read or write the in-memory ctypes object. However,
    in order to do n += 1, Python performs a read followed by a write, so a
    second process may read the old value before the new one is written by the
    first process. The solution is to use a multiprocessing.Lock to guarantee
    the atomicity of the modifications to Value.
    This class comes almost entirely from Eli Bendersky's blog:
    http://eli.thegreenplace.net/2012/01/04/shared-counter-with-pythons-multiprocessing/
    """

    def __init__(self, n: int = 0) -> None:
        self.count = multiprocessing.Value('i', n)

    def increment(self, n: int = 1) -> None:
        """ Increment the counter by n (default = 1) """
        with self.count.get_lock():
            self.count.value += n

    def decrement(self, n: int = 1) -> None:
        """ Decrement the counter by n (default = 1)"""
        with self.count.get_lock():
            self.count.value -= n

    @property
    def value(self) -> int:
        """ Return the value of the counter """
        return self.count.value


# class CollectorMultiprocessingQueue(JoinableQueue):

class CollectorMultiprocessingQueue(Queue):
    """ Devo main internal queue for Multiprocessing architecture """

    def __init__(self,
                 max_size_in_mb: int,
                 max_size_in_messages: int,
                 max_elapsed_time_in_sec: int,
                 max_wrap_size_in_messages: int,
                 generate_metrics: bool = False,
                 generate_collector_details: bool = False,
                 queue_name: str = None,
                 unlimited_privileges: bool = False):
        """Queue builder.
        :param max_size_in_mb: Hard limit in MB of the queue.
        :param max_size_in_messages: Hard limit in number of items of the queue.
        :param max_elapsed_time_in_sec: Used as bottleneck and timeout detection.
        :param max_wrap_size_in_messages: Max items in a wrap.
        :param generate_collector_details: Define the collector details level.
        :param queue_name: Custom name for the queue.
        :param unlimited_privileges: True if this queue should skip the max size validations. False if not.
        """
        super().__init__(ctx=multiprocessing.get_context())

        # # This is for avoiding any remaining internal thread related with queues
        # self.cancel_join_thread()

        if queue_name is None:
            queue_name = 'internal_queue'
        queue_suffix = '_multiprocessing'
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
        self.max_size_in_messages: SharedCounter = SharedCounter(max_size_in_messages)
        self.max_size_in_mb: SharedCounter = SharedCounter(max_size_in_mb)
        self.max_size_in_bytes: SharedCounter = SharedCounter(max_size_in_mb * 1024)
        self.max_elapsed_time_in_sec: SharedCounter = SharedCounter(max_elapsed_time_in_sec)
        self.max_wrap_size_in_items: SharedCounter = SharedCounter(max_wrap_size_in_messages)
        self.__generate_metrics: bool = generate_metrics
        self.__generate_collector_details: bool = generate_collector_details

        # Size counters
        self.queue_size_in_bytes: SharedCounter = SharedCounter(0)
        self.queue_size_in_messages: SharedCounter = SharedCounter(0)

        # Lockers ands events
        self.sleep_event: multiprocessing.Event = multiprocessing.Event()
        self.put_lock: multiprocessing.Lock = multiprocessing.Lock()
        self.updating_size_lock: multiprocessing.Lock = multiprocessing.Lock()
        self.input_lock: multiprocessing.Event = multiprocessing.Event()

        # Privileges
        self.unlimited_privileges: bool = unlimited_privileges

        # Set the proper "lock" status for not to be activated by default
        self.input_lock.set()

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
        start_time_local = datetime.utcnow()
        has_been_waiting = False
        queue_size_snapshot = None
        internal_queue_size_snapshot = None
        next_queue_size_snapshot = None

        self.input_lock.wait()

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

                queue_size_snapshot = self.qsize()
                internal_queue_size_snapshot = self.qsize()
                next_queue_size = queue_size_snapshot + number_of_messages
                next_queue_size_snapshot = next_queue_size

                while not self.__check_free_space(queue_item):
                    has_been_waiting = True
                    self.sleep_event.wait()
                    if self.sleep_event.is_set():
                        self.sleep_event.clear()

                if self.__generate_metrics:
                    queue_item.enqueued(self.queue_name)

                super().put(queue_item, block, timeout)
                with self.updating_size_lock:
                    self.queue_size_in_messages.increment(number_of_messages)
                    self.queue_size_in_bytes.increment(size_in_bytes)

        if self.__generate_collector_details:
            elapsed_seconds_putting = (datetime.utcnow() - start_time_local).total_seconds()

            if log.isEnabledFor(logging.DEBUG) \
                    and (has_been_waiting is True
                         or elapsed_seconds_before_put_lock > 1
                         or elapsed_seconds_putting > 1):
                log_message = \
                    f"[PUT][{self.queue_name}] " \
                    f"Elapsed seconds (before_lock/putting): " \
                    f"{elapsed_seconds_before_put_lock:0.3f}/" \
                    f"{elapsed_seconds_putting:0.3f}, " \
                    f"queue (size/sizeMB/messages/next_messages/maxsize/maxsizeMB): " \
                    f"{internal_queue_size_snapshot}/" \
                    f"{round(self.queue_size_in_bytes.value/1024/1024,2)}MB/" \
                    f"{queue_size_snapshot}/" \
                    f"{next_queue_size_snapshot}/" \
                    f"{self.max_size_in_messages.value}/" \
                    f"{round(self.max_size_in_bytes.value/1024/1024,2)}MB, " \
                    f"has_been_waiting: {has_been_waiting}"
                log.debug(log_message)

        if self.input_lock.is_set() is False:
            log.debug(f'[QUEUE] There was one item blocked in "{self.queue_name}"')

    def block_input_queue(self) -> None:
        """

        :return:
        """

        if self.input_lock.is_set() is True:
            log.info(
                f'[QUEUE] Blocking "{self.queue_name}" for avoiding the receiving of any new incoming message'
            )
            self.input_lock.clear()

    def release_input_queue(self) -> None:
        """

        :return:
        """

        if self.input_lock.is_set() is False:
            log.info(
                f'[QUEUE] Unblocking "{self.queue_name}" for continue receiving any incoming messages'
            )
            self.input_lock.set()

    def get(self, block: bool = True, timeout: int = None) -> CollectorQueueItem:
        """Gets new items from the queue and decrease the messages and bytes counter.

        :param timeout: Defines a timeout for this action.
        :param block: Defines if the queue must be blocked
        :return: An item that can be a MessageBath or str instance.
        """

        start_time_local: Optional[datetime] = None
        elapsed_seconds_before_get_lock = 0

        if self.__generate_collector_details:
            start_time_local = datetime.utcnow()
            elapsed_seconds_before_get_lock = (datetime.utcnow() - start_time_local).total_seconds()
            start_time_local = datetime.utcnow()

        # Get queue item and release the put event.waiter
        queue_item: CollectorQueueItem = super().get(block=block, timeout=timeout)
        self.sleep_event.set()

        number_of_messages = queue_item.number_of_messages
        size_in_bytes = queue_item.size_in_bytes

        with self.updating_size_lock:
            self.queue_size_in_messages.decrement(number_of_messages)
            self.queue_size_in_bytes.decrement(size_in_bytes)

        if self.__generate_collector_details:
            elapsed_seconds_getting = (datetime.utcnow() - start_time_local).total_seconds()

            if log.isEnabledFor(logging.DEBUG) and elapsed_seconds_getting > 1:
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

    def is_closed_and_empty(self) -> bool:
        """

        :return:
        """

        # print(f'{self.queue_name} -> input_lock: {self.input_lock.is_set() is False}, size: {self.qsize()}')
        return self.input_lock.is_set() is False and self.qsize() == 0

    def qsize(self) -> int:
        """ Returns the actual queue size in items """
        return self.queue_size_in_messages.value

    def get_maxsize(self) -> int:
        """ Returns the maximum size of the queue in items """
        return self.max_size_in_messages.value

    def empty(self) -> bool:
        """ Returns True if the queue is empty. False  if not """
        if self.queue_size_in_messages:
            return False
        return True

    def __getstate__(self):
        """ Helps to send the queue state (self/others) to other process """
        return {
            'parent_state': super().__getstate__(),
            'max_size_in_messages': self.max_size_in_messages,
            'max_size_in_bytes': self.max_size_in_bytes,
            'max_size_in_mb': self.max_size_in_mb,
            'max_elapsed_time_in_sec': self.max_elapsed_time_in_sec,
            'max_wrap_size_in_items': self.max_wrap_size_in_items,
            'queue_size_in_bytes': self.queue_size_in_bytes,
            'queue_size_in_messages': self.queue_size_in_messages,
            'sleep_event': self.sleep_event,
            'put_lock': self.put_lock,
            'input_lock': self.input_lock,
            'updating_size_lock': self.updating_size_lock,
            'queue_name': self.queue_name,
            '__generate_metrics': self.__generate_metrics,
            '__generate_collector_details': self.__generate_collector_details,
            'unlimited_privileges': self.unlimited_privileges
        }

    def __setstate__(self, state):
        """ Helps to send the queue state (self/others) to other process """
        super().__setstate__(state['parent_state'])
        self.max_size_in_messages = state['max_size_in_messages']
        self.max_size_in_bytes = state['max_size_in_bytes']
        self.max_size_in_mb = state['max_size_in_mb']
        self.max_elapsed_time_in_sec = state['max_elapsed_time_in_sec']
        self.max_wrap_size_in_items = state['max_wrap_size_in_items']
        self.queue_size_in_bytes = state['queue_size_in_bytes']
        self.queue_size_in_messages = state['queue_size_in_messages']
        self.sleep_event = state['sleep_event']
        self.put_lock = state['put_lock']
        self.input_lock = state['input_lock']
        self.updating_size_lock = state['updating_size_lock']
        self.queue_name = state['queue_name']
        self.__generate_metrics = state['__generate_metrics']
        self.__generate_collector_details = state['__generate_collector_details'],
        self.unlimited_privileges = state['unlimited_privileges']

    def __check_free_space(self, queue_item: CollectorQueueItem) -> bool:
        """Checks if the queue has enough free space taking in consideration the hard limits (items/mem_size).

        :param queue_item: CollectorQueueItem
        :return: True if the queue has enough space. False if not.
        """

        # Some queues have privileges!
        if self.unlimited_privileges is True:
            return True

        # Calculate
        new_size = self.queue_size_in_messages.value + queue_item.number_of_messages
        new_mem_size = self.queue_size_in_bytes.value + queue_item.size_in_bytes

        # Check
        size_validation = True if new_size <= self.max_size_in_messages.value else False
        mem_validation = True if new_mem_size <= self.max_size_in_bytes.value else False

        # Result
        if size_validation and mem_validation:
            return True

        return False

    def __wrap_item(self, item: MessageTypes) -> List[CollectorQueueItem]:
        if isinstance(item, (Message, LookupJob)):
            return [CollectorQueueItem(item)]

        elif isinstance(item, list):
            number_of_slides = math.ceil(len(item) / self.max_wrap_size_in_items.value)
            message_list = []
            for i in range(number_of_slides):
                slide_batch = item[i * self.max_wrap_size_in_items.value:(i + 1) * self.max_wrap_size_in_items.value]
                message_list.append(CollectorQueueItem(slide_batch))
            return message_list

        else:
            raise TypeError("Unexpected item type")

    def __repr__(self) -> str:
        """ String representation of the queue """
        str_representation: str = f'({self.__class__.__name__}) {self.queue_name} -> ' \
                                  f'max_size_in_messages: {self.max_size_in_messages.value}, ' \
                                  f'max_size_in_mb: {self.max_size_in_mb.value}, ' \
                                  f'max_wrap_size_in_items: {self.max_wrap_size_in_items.value}'

        return str_representation
