import hashlib
from collections import defaultdict
from typing import Dict, List, TypeAlias, Union, cast

from dependency_injector.wiring import Provide, inject
from event_core.adapters.services.embedding import (
    EmbeddingClient,
    QueryResponse,
)
from event_core.adapters.services.meta import AbstractMetaMapping, Meta
from event_core.adapters.services.storage import Payload, StorageClient
from event_core.domain.types import FileExt, Modal, UnitType, path_to_ext, PRIMITIVE_EXT_TO_MODAL

from bootstrap import DIContainer

ModalT: TypeAlias = str
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
    modal = PRIMITIVE_EXT_TO_MODAL[file_ext]
    storage[key] = Payload(data=data, obj_type=UnitType.DOC, modal=modal)
    meta[Meta.FILENAME][key] = file_name


@inject
def handle_query_text(
    user: str,
    text: str,
    top_n: int,
    embedder: EmbeddingClient = Provide[DIContainer.embedder],
    meta: AbstractMetaMapping = Provide[DIContainer.meta],
) -> Dict[ModalT, List[DocT]]:
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
    res: Dict[ModalT, List[DocT]] = {}
    query_res: QueryResponse = embedder.query_text(user, text, top_n)

    for modal, chunk_keys in query_res.modals.items():
        doc_chunks: Dict[str, List[str]] = defaultdict(list)

        for chunk_key in chunk_keys:
            doc_key = meta[Meta.PARENT][chunk_key]

            # for video chunks, return chunk's thumbnail instead
            if modal == Modal.VIDEO:
                chunk_key = meta[Meta.CHUNK_THUMB][chunk_key]
            doc_chunks[doc_key].append(chunk_key)

        res[modal] = [
            {
                "doc_key": doc_key,
                "doc_thumb_key": meta[Meta.DOC_THUMB][doc_key],
                "doc_filename": meta[Meta.FILENAME][doc_key],
                "chunk_keys": chunk_keys,
            }
            for doc_key, chunk_keys in doc_chunks.items()
        ]
    return res


@inject
def handle_object_get(
    path: str,
    storage: StorageClient = Provide[DIContainer.storage],
) -> bytes:
    return storage[path]
