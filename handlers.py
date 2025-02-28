import hashlib
from typing import Dict, List, cast

from dependency_injector.wiring import Provide, inject
from event_core.adapters.services.embedding import EmbeddingClient
from event_core.adapters.services.storage import StorageClient
from event_core.domain.types import MODAL_FACTORY, FileExt, ObjectType

from bootstrap import DIContainer


def _get_file_ext(file_name: str) -> FileExt:
    suffix = "." + file_name.rsplit(".", 1)[1]
    return cast(FileExt, FileExt._value2member_map_[suffix])


def _hash(s: str) -> str:
    return hashlib.sha256(s.encode()).hexdigest()


@inject
def handle_add(
    data: bytes,
    file_name: str,
    user: str,
    storage_client: StorageClient = Provide[DIContainer.storage_client],
) -> None:
    file_ext = _get_file_ext(file_name)
    modal = MODAL_FACTORY[file_ext]
    hash = _hash(file_name)
    key = f"{user}/{hash}{file_ext}"
    storage_client.add(data, key, ObjectType.DOC, modal)


@inject
def handle_query_text(
    user: str,
    text: str,
    n_cands: int,
    n_rank: int,
    emb_client: EmbeddingClient = Provide[DIContainer.embedding_client],
) -> Dict[str, List[str]]:
    return emb_client.query_text(user, text, n_cands, n_rank)


@inject
def handle_object_get(
    obj_path: str,
    storage_client: StorageClient = Provide[DIContainer.storage_client],
) -> bytes:
    return storage_client.get(obj_path)
