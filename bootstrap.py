from dependency_injector import containers, providers
from event_core.adapters.services.embedding import EmbeddingAPIClient
from event_core.adapters.services.storage import StorageAPIClient
from event_core.adapters.services.mapping import RedisMapper

MODULES = ("handlers",)


class DIContainer(containers.DeclarativeContainer):
    storage_client = providers.Singleton(StorageAPIClient)
    embedding_client = providers.Singleton(EmbeddingAPIClient)
    mapper = providers.Singleton(RedisMapper)


def bootstrap() -> None:
    container = DIContainer()
    container.wire(modules=MODULES)
