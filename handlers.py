import hashlib
from typing import List

from event_core.adapters.services.api.embedding import query_text
from event_core.adapters.services.api.storage import add, get
from event_core.domain.types import ObjectType


def handle_add(data: bytes, file_name: str, user: str) -> None:
    ext = file_name.rsplit(".", 1)[1]
    hash = hashlib.sha256(file_name.encode()).hexdigest()
    obj_path = f"{user}/{hash}.{ext}"
    add(data, obj_path, ObjectType.DOC)


def handle_query_text(user: str, text: str) -> List[str]:
    return query_text(user, text)


def handle_object_get(obj_path: str) -> bytes:
    return get(obj_path)
