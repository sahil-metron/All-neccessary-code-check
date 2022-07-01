# -*- coding: utf-8 -*-
import json
import platform
import logging
import multiprocessing
import sys
import threading
from datetime import datetime
from pathlib import Path
from typing import Union, Type, Optional

import click as click

from agent.collector import CollectorThread
from agent.commons.collector_utils import CollectorUtils
from agent.commons.constants import Constants
from agent.configuration.configuration import CollectorConfiguration, CollectorConfigurationException
from agent.controllers.multiprocessing.input_controller_process import InputMultiprocessingController
from agent.controllers.multiprocessing.output_controller_process import OutputMultiprocessingController
from agent.controllers.multithreading.input_mt_controller import InputMultithreadingController
from agent.controllers.multithreading.output_mt_controller import OutputMultithreadingController
from agent.queues.collector_multiprocessing_queue import CollectorMultiprocessingQueue
from agent.queues.collector_multithreading_queue import CollectorMultithreadingQueue

APPNAME = 'devo-collector-base'
FORMAT_DATETIME = '%Y-%m-%d %H:%M:%S.%f'
FLAG_FORMAT = "YYYY-MM-DD HH:MM:SS.nnnnnnnnn"
FLAG_REGEX = r"[0-9]{4}-(?:0[1-9]|1[0-2])-(?:0[1-9]|[1-2][0-9]|3[0-1]) " \
             r"(?:2[0-3]|[01][0-9]):[0-5][0-9](?::[0-5][0-9])?(?:\.\d{1,9})?"

# Global definitions
log = logging.getLogger(__name__)
log_level = logging.INFO

QueueType = Union[CollectorMultiprocessingQueue, CollectorMultithreadingQueue]
InputType = Union[InputMultiprocessingController, InputMultithreadingController]
OutputType = Union[OutputMultiprocessingController, OutputMultithreadingController]
EventType = Union[multiprocessing.Event, threading.Event]


def send_internal_collector_message(internal_queue: QueueType,
                                    message_content: Union[str, dict],
                                    level: str = None,
                                    shared_domain: bool = False):
    """
    This function add a new message to the internal collector.
    :param internal_queue: transport message queue
    :param message_content: Message to be sent to Devo.
    :param level: Severity of the message.
    :param shared_domain: True if the message has <shared_domain> property to be watched by IF Devo Team for monitoring
        purposes.
    """

    CollectorUtils.send_internal_collector_message(
        internal_queue,
        message_content,
        level=level,
        shared_domain=shared_domain
    )


def wait_until_all_threads_are_stopped() -> None:
    """

    :return:
    """

    wait_object: threading.Event = threading.Event()
    still_threads_alive = True
    while still_threads_alive:
        alive_threads = []
        for t in threading.enumerate():
            if t.is_alive() and t.name != "MainThread":
                alive_threads.append(t.name)

        if alive_threads:
            log.warning(f'There are some alive_threads: {json.dumps(alive_threads)}')
            called: bool = wait_object.wait(timeout=1)
            if called is True:
                wait_object.clear()
        else:
            still_threads_alive = False


def show_devo_banner() -> None:
    """ Show the Devo Marketing banner"""
    print(Constants.BANNER_4)
    sys.stdout.flush()


@click.command()
@click.option('--config', 'config_full_filename',
              help="Path to full structured configuration file "
                   "(with sections \"globals\", \"inputs\" and \"outputs\" inside)")
@click.option('--job_config_loc', 'config_inputs_filename',
              help="Path to collector \"local\" configuration file for being used with the Collector Server")
@click.option('--collector_config_loc', 'config_global_filename',
              help="Path to collector \"internal\" configuration file for being used with the Collector Server")
@click.option('--no-save-state',
              default=False,
              is_flag=True,
              help="Do not persist the state")
def cli(
        config_inputs_filename,
        config_global_filename,
        config_full_filename,
        no_save_state):
    try:

        global log_level

        # Create configuration object
        conf = CollectorConfiguration(
            config_full_filename,
            config_inputs_filename,
            config_global_filename,
            no_save_state
        )
        CollectorUtils.extract_collector_metadata(conf)

        # Get debug status
        if conf.config["globals"].get("debug", False):
            log_level = logging.DEBUG

        # Config logging service
        logging.basicConfig(
            level=log_level,
            format='%(asctime)s.%(msecs)03d %(levelname)7s %(processName)s::%(threadName)s -> %(message)s',
            datefmt='%Y-%m-%dT%H:%M:%S'
        )

        # Build the internal queues <output and lookup> arguments and builders
        queue_arguments: dict = conf.get_queue_building_arguments()
        multiprocessing_mode: bool = conf.is_multiprocessing()
        if multiprocessing_mode:
            queue_builder: Type[CollectorMultiprocessingQueue] = CollectorMultiprocessingQueue
        else:
            queue_builder: Type[CollectorMultithreadingQueue] = CollectorMultithreadingQueue

        # Build standard queue
        queue_arguments['queue_name'] = 'standard_queue'
        standard_queue = queue_builder(**queue_arguments)

        # Build lookup queue
        queue_arguments['queue_name'] = 'lookup_queue'
        queue_arguments['unlimited_privileges'] = True
        lookup_queue = queue_builder(**queue_arguments)

        # Build internal queue
        queue_arguments['queue_name'] = 'internal_queue'
        queue_arguments['unlimited_privileges'] = False
        queue_arguments["max_size_in_messages"] = 10000
        queue_arguments["max_wrap_size_in_messages"] = 100
        internal_queue = queue_builder(**queue_arguments)

        build_time_file = Path("build_time.txt")
        build_time: str = "UNKNOWN"
        if build_time_file.is_file():
            try:
                with open("build_time.txt", "r") as build_time_file:
                    build_time = build_time_file.read().splitlines()[-1]
            except Exception as ex:
                log.warning(f'Problems reading "build_time.txt" file: {str(ex)}')

        # Log trace
        now_utc = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        if log.isEnabledFor(logging.INFO):
            log_message = \
                {
                    "build_time": build_time,
                    "os_info": platform.platform(),
                    "collector_name": CollectorUtils.collector_name,
                    "collector_version": CollectorUtils.collector_version,
                    "collector_owner": CollectorUtils.collector_owner,
                    "started_at": now_utc
                }
            log.info(json.dumps(log_message))
            send_internal_collector_message(internal_queue, log_message, level="info", shared_domain=True)

        log.info(standard_queue)

        # Say <hello world!>
        log_message_1 = f"[MAIN] Configuration content: {conf}"
        log_message_2 = f"[MAIN] Queue configuration: {standard_queue.__repr__()}"
        log.debug(log_message_1)
        log.debug(log_message_2)
        if log.isEnabledFor(logging.DEBUG):
            send_internal_collector_message(internal_queue, log_message_1, level="debug")
            send_internal_collector_message(internal_queue, log_message_2, level="debug")

        # Execute collector

        collector: CollectorThread = \
            CollectorThread(
                config=conf,
                standard_queue=standard_queue,
                lookup_queue=lookup_queue,
                internal_queue=internal_queue
             )
        collector.start()

        collector.join()

        # Log trace
        now_utc = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")

        log.warning('Finalizing the whole collector')

        if log.isEnabledFor(logging.INFO):
            log_message = \
                {
                    "collector_name": CollectorUtils.collector_name,
                    "collector_version": CollectorUtils.collector_version,
                    "collector_owner": CollectorUtils.collector_owner,
                    "stopped_at": now_utc
                }
            log.info(json.dumps(log_message))

        wait_until_all_threads_are_stopped()

        log.info(f'[EXIT] Process "MainProcess" completely terminated')

    except CollectorConfigurationException as ex:
        log.error(ex)
        sys.exit(-1)

    sys.exit()


if __name__ == "__main__":
    show_devo_banner()
    cli()
