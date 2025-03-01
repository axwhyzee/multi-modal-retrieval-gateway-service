from dependency_injector import containers, providers
from event_core.adapters.services.embedding import EmbeddingAPIClient
from event_core.adapters.services.meta import RedisMetaMapping
from event_core.adapters.services.storage import StorageAPIClient

MODULES = ("handlers",)


class DIContainer(containers.DeclarativeContainer):
    storage = providers.Singleton(StorageAPIClient)
    embedder = providers.Singleton(EmbeddingAPIClient)
    meta = providers.Singleton(RedisMetaMapping)


def bootstrap() -> None:
    container = DIContainer()
    container.wire(modules=MODULES)
