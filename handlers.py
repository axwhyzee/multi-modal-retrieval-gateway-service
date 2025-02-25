import hashlib
from typing import Dict, List

from event_core.adapters.services.api.embedding import query_text
from event_core.adapters.services.api.storage import add, get
from event_core.domain.types import ObjectType


def handle_add(data: bytes, file_name: str, user: str) -> None:
    ext = file_name.rsplit(".", 1)[1]
    hash = hashlib.sha256(file_name.encode()).hexdigest()
    key = f"{user}/{hash}.{ext}"
    add(data, key, key, ObjectType.DOC)


def handle_query_text(
    user: str, text: str, n_cands: int, n_rank: int
) -> Dict[str, List[str]]:
    return query_text(user, text, n_cands, n_rank)


def handle_object_get(obj_path: str) -> bytes:
    return get(obj_path)
