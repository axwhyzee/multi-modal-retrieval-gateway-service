import hashlib
from collections import defaultdict
from typing import Dict, List, TypeAlias, Union

from dependency_injector.wiring import Provide, inject
from event_core.adapters.services.embedding import EmbeddingClient
from event_core.adapters.services.meta import AbstractMetaMapping, Meta
from event_core.adapters.services.storage import Payload, StorageClient
from event_core.domain.types import EXT_TO_MODAL, Modal, UnitType, path_to_ext

from bootstrap import DIContainer

DocT: TypeAlias = Dict[str, Union[str, List[str]]]


def _hash(s: str) -> str:
    return hashlib.sha256(s.encode()).hexdigest()


@inject
def handle_add(
    data: bytes,
    file_name: str,
    user: str,
    storage: StorageClient = Provide[DIContainer.storage],
    meta: AbstractMetaMapping = Provide[DIContainer.meta],
) -> None:
    """
    1. Generate key from path
    2. Store doc in Storage Service
    3. Store doc key -> doc name in Meta Service
    """
    hash = _hash(file_name)
    file_ext = path_to_ext(file_name)
    key = f"{user}/{hash}{file_ext}"
    storage[key] = Payload(data=data, type=UnitType.DOC)
    meta[Meta.FILENAME][key] = file_name


@inject
def handle_query_text(
    user: str,
    text: str,
    top_n: int,
    embedder: EmbeddingClient = Provide[DIContainer.embedder],
    meta: AbstractMetaMapping = Provide[DIContainer.meta],
) -> List[DocT]:
    """
    1. Forward query to Embedding Service, receiving top_n
       most relevant chunk keys for each modal as response
    2. Match chunks to their corresponding parent doc
    3. For each doc, fetch the original doc filename, as
       well as the doc thumbnail key. Each doc will
       also contain a list of chunk keys
    3. Return a dictionary containing docs categorized by
       their associated modal
    """
    chunk_keys = embedder.query_text(user, text, top_n)
    docs: Dict[str, List[str]] = defaultdict(list)  # map docs to its chunks

    for chunk_key in chunk_keys:
        doc_key = meta[Meta.PARENT][chunk_key]
        chunk_file_ext = path_to_ext(chunk_key)
        chunk_modal = EXT_TO_MODAL[chunk_file_ext]

        # for images, return key of thumbnail instead
        if chunk_modal == Modal.IMAGE:
            chunk_key = meta[Meta.CHUNK_THUMB][chunk_key]
        docs[doc_key].append(chunk_key)

    return [
        {
            "doc_key": doc_key,
            "doc_thumb_key": meta[Meta.DOC_THUMB][doc_key],
            "doc_filename": meta[Meta.FILENAME][doc_key],
            "chunk_keys": chunk_keys,
        }
        for doc_key, chunk_keys in docs.items()
    ]


@inject
def handle_get(
    path: str,
    storage: StorageClient = Provide[DIContainer.storage],
) -> bytes:
    return storage[path]
