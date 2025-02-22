import hashlib
from typing import List

from event_core.adapters.services.api.embedding import query_text
from event_core.adapters.services.api.storage import add
from event_core.domain.types import ObjectType


def handle_add(data: bytes, file_name: str, user: str) -> None:
    hash = hashlib.sha256(file_name.encode()).hexdigest()
    obj_path = f'{user}/{hash}'
    add(data, obj_path, ObjectType.DOC)


def handle_text_query(user: str, text: str) -> List[str]:
    return query_text(user, text)
