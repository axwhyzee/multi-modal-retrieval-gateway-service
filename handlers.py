import hashlib
from dataclasses import asdict, dataclass
from collections import defaultdict
from typing import Dict, List, cast, Optional, Any

from dependency_injector.wiring import Provide, inject
from event_core.adapters.services.embedding import EmbeddingClient
from event_core.adapters.services.storage import StorageClient
from event_core.domain.types import MODAL_FACTORY, FileExt, ObjectType, Modal
from event_core.adapters.services.mapping import AbstractMapper

from bootstrap import DIContainer


@dataclass
class ObjPresentation:
    doc_key: str
    doc_thumb_key: str
    doc_filename: str
    chunk_keys: Optional[List[str]]
   


class MetaType(ObjectType):
    FILENAME = "FILENAME"



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
    mapper: AbstractMapper = Provide[DIContainer.mapper]
) -> None:
    file_ext = _get_file_ext(file_name)
    modal = MODAL_FACTORY[file_ext]
    hash = _hash(file_name)
    key = f"{user}/{hash}{file_ext}"
    storage_client.add(data, key, ObjectType.DOC, modal)
    mapper.add(ObjectType.DOC, MetaType.FILENAME, key, file_name)


@inject
def handle_query_text(
    user: str,
    text: str,
    n_cands: int,
    n_rank: int,
    emb_client: EmbeddingClient = Provide[DIContainer.embedding_client],
    mapper: AbstractMapper = Provide[DIContainer.mapper]
) -> Dict[str, List[Dict[str, Any]]]:
    
    res: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    keys_by_modal = emb_client.query_text(user, text, n_cands, n_rank)
    
    for modal, chunk_keys in keys_by_modal.items():
        chunks_by_doc: Dict[str, List[str]] = defaultdict(list)

        for chunk_key in chunk_keys:
            doc_key = mapper.get(ObjectType.CHUNK, ObjectType.DOC, chunk_key)

            if modal == Modal.VIDEO:
                chunk_key = mapper.get(ObjectType.CHUNK, ObjectType.CHUNK_THUMBNAIL, chunk_key)
            chunks_by_doc[doc_key].append(chunk_key)

        for doc_key, chunk_keys in chunks_by_doc.items():
            doc_filename = mapper.get(ObjectType.DOC, MetaType.FILENAME, doc_key)
            doc_thumb_key = mapper.get(ObjectType.DOC, ObjectType.DOC_THUMBNAIL, doc_key)
            res[modal].append(
                asdict(
                    ObjPresentation(
                        doc_key=doc_key,
                        doc_thumb_key=doc_thumb_key,
                        doc_filename=doc_filename,
                        chunk_keys=chunk_keys
                    )
                )
            )
    return res        


@inject
def handle_object_get(
    obj_path: str,
    storage_client: StorageClient = Provide[DIContainer.storage_client],
) -> bytes:
    return storage_client.get(obj_path)
