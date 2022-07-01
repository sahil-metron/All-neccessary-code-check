import logging

from agent.persistence.base_manager import CollectorPersistenceManager
from agent.persistence.redis.redis import RedisPersistence

log = logging.getLogger(__name__)


class RedisPersistenceManager(CollectorPersistenceManager):

    def __init__(self, collector_id: str, persistence_config: dict):
        super().__init__(collector_id, "redis")
        self.host: str = persistence_config.get("host")
        if self.host is None:
            self.host = persistence_config.get("location")
        self.port: int = persistence_config.get("port")
        self.password: str = persistence_config.get("password")
        self.db: str = persistence_config.get("db")
        self.socket_timeout: int = persistence_config.get("socket_timeout")

    def get_instance_for_unique_identifier(self, unique_identifier: str) -> RedisPersistence:
        return RedisPersistence(
            self.collector_id,
            unique_identifier,
            self.host,
            self.port,
            self.db,
            self.password,
            self.socket_timeout
        )

    def __str__(self):
        return \
            "{{" \
            "\"class\": \"{}\", " \
            "\"config\": " \
            "{{" \
            "\"host\": {}, " \
            "\"port\": {}, " \
            "\"db\": {}, " \
            "\"password\": {}, " \
            "\"socket_timeout\": {}" \
            "}}" \
            "}}".format(
                type(self).__name__,
                "\"{}\"".format(self.host) if self.host else "null",
                self.port if self.port else "null",
                "\"{}\"".format(self.db) if self.db else "null",
                "\"{}\"".format(self.password) if self.password else "null",
                self.socket_timeout if self.socket_timeout else "null"
            )
