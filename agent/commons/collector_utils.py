import json
from datetime import datetime
from typing import Union

import pytz

from agent.commons.constants import Constants
from agent.configuration.configuration import CollectorConfiguration
from agent.message.message import Message
from agent.queues.collector_multiprocessing_queue import CollectorMultiprocessingQueue
from agent.queues.collector_multithreading_queue import CollectorMultithreadingQueue

QueueType = Union[CollectorMultiprocessingQueue, CollectorMultithreadingQueue]


class CollectorUtils:

    collector_name = "unknown"
    collector_version = "unknown"
    collector_owner = "integrations_factory@devo.com"

    @staticmethod
    def send_internal_collector_message(
            output_queue: QueueType,
            message_content: Union[str, dict],
            input_name: str = None,
            service_name: str = None,
            module_name: str = None,
            level: str = None,
            shared_domain: bool = False) -> None:
        if level is None:
            level = "default"

        message_timestamp = datetime.utcnow().replace(tzinfo=pytz.utc)

        message_tag = \
            "{base_tag}.{visibility}.{level}".format(
                base_tag=Constants.INTERNAL_MESSAGE_TAGGING_BASE,
                visibility="global" if shared_domain else "local",
                level=level.lower()
            )

        message_content_dict: dict = {}
        if isinstance(message_content, str):
            message_content_dict = {
                "msg": message_content
            }
        elif isinstance(message_content, dict):
            message_content_dict = message_content

        if "time" not in message_content_dict:
            message_content_dict["time"] = message_timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        if "level" not in message_content_dict:
            message_content_dict["level"] = level
        if "collector_name" not in message_content_dict:
            message_content_dict["collector_name"] = CollectorUtils.collector_name
        if "collector_version" not in message_content_dict:
            message_content_dict["collector_version"] = CollectorUtils.collector_version
        if "collector_image" not in message_content_dict:
            message_content_dict["collector_image"] = Constants.COLLECTOR_IMAGE
        if input_name and "input_name" not in message_content_dict:
            message_content_dict["input_name"] = input_name
        if service_name and "service_name" not in message_content_dict:
            message_content_dict["service_name"] = service_name
        if module_name and "module_name" not in message_content_dict:
            message_content_dict["module_name"] = module_name
        if shared_domain and "shared_domain" not in message_content_dict:
            message_content_dict["shared_domain"] = True

        output_queue.put(
            Message(
                message_timestamp,
                message_tag,
                json.dumps(message_content_dict),
                is_internal=True
            )
        )

    @classmethod
    def extract_collector_metadata(cls, conf: CollectorConfiguration):
        collector_name = conf.get_collector_name()
        if collector_name:
            cls.collector_name = collector_name

        collector_version = conf.get_collector_version()
        if collector_version:
            cls.collector_version = collector_version
        else:
            conf.collector_version = cls.collector_version

        collector_owner = conf.get_collector_owner()
        if collector_owner:
            cls.collector_owner = collector_owner
        else:
            conf.collector_owner = cls.collector_owner

    @staticmethod
    def human_readable_size(size, decimal_places=2) -> str:
        for unit in ['B', 'KiB', 'MiB', 'GiB', 'TiB']:
            if size < 1024.0:
                break
            size /= 1024.0
        return f"{size:.{decimal_places}f}{unit}"
