import hashlib
import json
import logging
import os
import pickle
from datetime import datetime

from agent.persistence.base import CollectorPersistence
from agent.persistence.exceptions.exceptions import ObjectPersistenceException, SavePersistenceException

log = logging.getLogger(__name__)


class FilesystemPersistence(CollectorPersistence):

    def __init__(self, directory_base: str, collector_id: str, unique_identifier: str) -> None:
        super().__init__(collector_id, unique_identifier)
        if directory_base is None:
            raise ObjectPersistenceException(1, f"Directory name property is not valid, value: {directory_base}")

        self.credentials_filename_absolute_path = directory_base
        if os.path.isabs(self.credentials_filename_absolute_path) is False:
            self.credentials_filename_absolute_path = \
                os.path.join(os.getcwd(), self.credentials_filename_absolute_path)

        if self.use_old_format:
            filename_hash = hashlib.md5(unique_identifier.encode("utf-8"))
            self.filename_path = \
                os.path.abspath(os.path.join(self.credentials_filename_absolute_path, filename_hash.hexdigest()))
        else:
            filename_hash = unique_identifier + ".json"
            self.filename_path = \
                os.path.abspath(os.path.join(self.credentials_filename_absolute_path, collector_id, filename_hash))

        if not os.path.isdir(os.path.dirname(self.filename_path)):
            os.makedirs(os.path.dirname(self.filename_path))
        try:
            open(self.filename_path, 'a').close()
        except Exception as ex:
            raise ObjectPersistenceException(
                2,
                f"Problems with persistence file \"{self.filename_path}\", details: {ex}"
            )
        self.last_known_state = None

    def _save_state(self, object_to_save: dict, no_log_traces: bool):
        file_mode = "w"
        if self.use_old_format:
            file_mode = "wb"
        file = open(self.filename_path, mode=file_mode)
        try:
            if self.use_old_format:
                pickle.dump(object_to_save, file, pickle.HIGHEST_PROTOCOL)
            else:
                json.dump(object_to_save, file)
            file.close()
            self.last_known_state = object_to_save
            self._last_persisted_timestamp = datetime.now()
            if no_log_traces is False:
                log.debug("{} -> State saved: {}".format(type(self).__name__, self.last_known_state))
        except Exception as ex:
            if no_log_traces is False:
                log.debug(f"{type(self).__name__} -> Nothing saved to persistence state object")
            raise SavePersistenceException(1, str(ex))

    def _load_state(self, no_log_traces: bool) -> dict:
        file_mode = "r"
        if self.use_old_format:
            file_mode = "rb"
        file = open(self.filename_path, mode=file_mode)
        try:
            if self.use_old_format:
                object_loaded = pickle.load(file)
            else:
                object_loaded = json.load(file)
            file.close()
            self.last_known_state = object_loaded
            if no_log_traces is False:
                log.debug("{} -> State loaded: {}".format(type(self).__name__, self.last_known_state))
            return object_loaded
        except Exception:
            if no_log_traces is False:
                log.debug(f"{type(self).__name__} -> Nothing read from persistence state object")

    def _load_state_old_format(self, no_log_traces: bool, unique_identifier) -> dict:
        if unique_identifier:
            filename_hash = hashlib.md5(unique_identifier.encode("utf-8"))
            filename_path = \
                os.path.abspath(os.path.join(self.credentials_filename_absolute_path, filename_hash.hexdigest()))
        else:
            filename_path = self.filename_path

        file = open(filename_path, mode="rb")
        try:
            object_loaded = pickle.load(file)
            file.close()
            self.last_known_state = object_loaded
            if no_log_traces is False:
                log.debug("{} -> State loaded: {}".format(type(self).__name__, self.last_known_state))
            return object_loaded
        except Exception:
            if no_log_traces is False:
                log.debug(f"{type(self).__name__} -> Nothing read from persistence state object")

    def _is_ok(self) -> bool:
        if os.path.exists(self.filename_path) and os.path.isfile(self.filename_path):
            return os.access(self.filename_path, os.W_OK)
        return False

    def __str__(self):
        return \
            f'{{' \
            f'"unique_identifier": {self.unique_identifier}, ' \
            f'"class": "{type(self).__name__}", ' \
            f'"filename_path": {self.filename_path}, ' \
            f'"last_known_state": {self.last_known_state} ' \
            f'}}'
