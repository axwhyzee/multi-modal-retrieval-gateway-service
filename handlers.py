import hashlib
from collections import defaultdict
from typing import Any, Dict, List, Optional, TypeAlias

from dependency_injector.wiring import Provide, inject
from event_core.adapters.services.embedding import EmbeddingClient
from event_core.adapters.services.meta import AbstractMetaMapping, Meta
from event_core.adapters.services.storage import Payload, StorageClient
from event_core.domain.types import Asset, FileExt, path_to_ext

from bootstrap import DIContainer

DocT: TypeAlias = Dict[str, Any]


ELEM_META = (
    Meta.PAGE,
    Meta.COORDS,
    Meta.FRAME_SECONDS,
)

IMG_EXTS = (
    FileExt.JPEG,
    FileExt.JPG,
    FileExt.PDF,
)


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
    storage[key] = Payload(data=data, type=Asset.DOC)
    meta[Meta.FILENAME][key] = file_name


@inject
def handle_query_text(
    user: str,
    text: str,
    top_n: int,
    exclude_elems: Optional[List[str]] = None,
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
    chunk_keys = embedder.query_text(user, text, top_n, exclude_elems)
    docs: Dict[str, List[str]] = defaultdict(list)  # map docs to its chunks

    for chunk_key in chunk_keys:
        doc_key = meta[Meta.PARENT][chunk_key]
        chunk_file_ext = path_to_ext(chunk_key)

        # for images, return key of thumbnail instead
        if chunk_file_ext in IMG_EXTS:
            chunk_key = meta[Meta.CHUNK_THUMB][chunk_key]
        docs[doc_key].append(chunk_key)

    return [
        {
            "doc_key": doc_key,
            "doc_thumb_key": meta[Meta.DOC_THUMB][doc_key],
            "doc_filename": meta[Meta.FILENAME][doc_key],
            "chunks": [
                {
                    "meta": {
                        meta_key.value: meta[meta_key][chunk_key]
                        for meta_key in ELEM_META
                        if chunk_key in meta[meta_key]
                    },
                    "key": chunk_key,
                }
                for chunk_key in chunk_keys
            ],
        }
        for doc_key, chunk_keys in docs.items()
    ]


@inject
def handle_get(
    path: str,
    storage: StorageClient = Provide[DIContainer.storage],
) -> bytes:
    return storage[path]


@inject
def handle_list(
    storage: StorageClient = Provide[DIContainer.storage],
) -> List[str]:
    return list(storage)
