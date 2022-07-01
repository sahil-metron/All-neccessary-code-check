import json
import logging
from datetime import datetime

import redis

from agent.persistence.base import CollectorPersistence
from agent.persistence.exceptions.exceptions import SavePersistenceException, LoadPersistenceException, \
    ObjectPersistenceException

log = logging.getLogger(__name__)


class RedisPersistence(CollectorPersistence):

    def __init__(self,
                 collector_id: str,
                 unique_identifier: str,
                 host: str,
                 port: int,
                 db: str,
                 password: str,
                 socket_timeout: int) -> None:
        super().__init__(collector_id, unique_identifier)
        if host is None or port is None:
            raise ObjectPersistenceException(
                0,
                "At least one of mandatory parameters is having "
                "value \"None\": host: \"{}\", port: {}".format(
                    host,
                    port
                )
            )
        if db is None:
            db = 0
        self.redis_instance = redis.StrictRedis(
            host=host,
            port=port,
            db=db,
            password=password,
            socket_timeout=socket_timeout
        )

        self.last_known_state = None

    def _save_state(self, object_to_save: dict, no_log_traces: bool):
        try:
            json_object_to_save = json.dumps(object_to_save)
            if self.use_old_format:
                redis_response = \
                    self.redis_instance.set(
                        self.unique_identifier,
                        json_object_to_save
                    )
            else:
                redis_response = \
                    self.redis_instance.hset(
                        self.collector_id,
                        self.unique_identifier,
                        json_object_to_save
                    )
            self.last_known_state = object_to_save
            self._last_persisted_timestamp = datetime.now()
            if no_log_traces is False:
                log.debug("{} -> State saved: {}".format(type(self).__name__, self.last_known_state))
        except Exception as ex:
            if no_log_traces is False:
                log.debug(f"{type(self).__name__} -> Nothing saved to persistence state object")
            raise SavePersistenceException(1, str(ex))

    def _load_state(self, no_log_traces: bool) -> dict:
        try:
            if self.use_old_format:
                state = self.redis_instance.get(self.unique_identifier)
            else:
                state = self.redis_instance.hget(self.collector_id, self.unique_identifier)
            if state:
                self.last_known_state = state
                if no_log_traces is False:
                    log.debug("{} -> State loaded: {}".format(type(self).__name__, self.last_known_state))
                return json.loads(state)
        except Exception as ex:
            if no_log_traces is False:
                log.debug(f"{type(self).__name__} -> Nothing read from persistence state object")
            raise LoadPersistenceException(1, str(ex))

    def _load_state_old_format(self, no_log_traces: bool, unique_identifier) -> dict:
        if unique_identifier is None:
            unique_identifier = self.unique_identifier
        try:
            state = self.redis_instance.get(unique_identifier)
            if state:
                self.last_known_state = state
                if no_log_traces is False:
                    log.debug("{} -> State loaded: {}".format(type(self).__name__, self.last_known_state))
                return json.loads(state)
        except Exception as ex:
            if no_log_traces is False:
                log.debug(f"{type(self).__name__} -> Nothing read from persistence state object")
            raise LoadPersistenceException(1, str(ex))

    def _is_ok(self) -> bool:
        try:
            self.redis_instance.ping()
            return True
        except Exception:
            return False

    def __str__(self):
        return \
            "{{" \
            "\"unique_identifier\": {}, " \
            "\"class\": \"{}\", " \
            "\"last_known_state\": {}" \
            "}}".format(
                "\"{}\"".format(self.unique_identifier) if self.unique_identifier else "null",
                type(self).__name__,
                self.last_known_state if self.last_known_state else "null"
            )
