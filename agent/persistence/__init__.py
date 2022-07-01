from .filesystem.filesystem_manager import FilesystemPersistenceManager
from .memory.memory_manager import MemoryPersistenceManager
from .redis.redis_manager import RedisPersistenceManager

persistence_types = {
    'redis': RedisPersistenceManager,
    'memory': MemoryPersistenceManager,
    'filesystem': FilesystemPersistenceManager,
    'json': FilesystemPersistenceManager
}
