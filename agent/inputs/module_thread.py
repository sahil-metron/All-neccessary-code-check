import logging
from abc import ABC
from threading import Thread
from typing import Union

from agent.commons.collector_utils import CollectorUtils
from agent.persistence.persistence_manager import PersistenceManager
from agent.queues.collector_multiprocessing_queue import CollectorMultiprocessingQueue
from agent.queues.collector_multithreading_queue import CollectorMultithreadingQueue

log = logging.getLogger(__name__)

QueueType = Union[CollectorMultiprocessingQueue, CollectorMultithreadingQueue]


class ModuleThread(Thread, ABC):

    def __init__(self,
                 parent_thread,
                 input_id: str,
                 input_name: str,
                 input_config: dict,
                 input_definition: dict,
                 service_name: str,
                 service_type: str,
                 service_config: dict,
                 service_definition: dict,
                 module_name: str,
                 module_config: dict,
                 module_definition: dict,
                 persistence_manager: PersistenceManager,
                 output_queue: QueueType,
                 lookup_queue: QueueType,
                 internal_queue: QueueType,
                 submodule_name: str = None,
                 submodule_config=None) -> None:
        """

        :param parent_thread:
        :param input_id:
        :param input_name:
        :param input_config:
        :param input_definition:
        :param service_name:
        :param service_type:
        :param service_config:
        :param service_definition:
        :param module_name:
        :param module_config:
        :param module_definition:
        :param persistence_manager:
        :param output_queue:
        :param lookup_queue:
        :param internal_queue:
        :param submodule_name:
        :param submodule_config:
        """
        super().__init__()

        # TODO Review if this code is really needed
        self.debug: bool = bool(input_config.get("debug"))
        if self.debug:
            log.setLevel(logging.DEBUG)

        submodule_id = ""
        if isinstance(submodule_config, str):
            submodule_id = f",{submodule_config}"
        elif submodule_name:
            submodule_id = f",{submodule_name}"
        self.name = \
            self.__class__.__name__ \
            + f"({CollectorUtils.collector_name}," \
              f"{input_name}#{input_id}," \
              f"{service_name}#{service_type}{submodule_id})"
