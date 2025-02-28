from typing import cast

import pytest
from event_core.adapters.services.storage import FakeStorageClient

from bootstrap import MODULES, DIContainer
from handlers import handle_add


@pytest.fixture
def container() -> DIContainer:
    container = DIContainer()
    container.storage_client.override(FakeStorageClient())
    container.wire(modules=MODULES)
    return container


@pytest.fixture
def test_data() -> bytes:
    return b"test content"


@pytest.mark.parametrize(
    "user,file_name",
    (
        (
            "user1",
            "test_image.jpg",
        ),
        (
            "user2",
            "test_video.mp4",
        ),
        (
            "user3",
            "test_text.txt",
        ),
    ),
)
def test_handle_add_adds_object_of_correct_key(
    user: str,
    file_name: str,
    container: DIContainer,
    test_data: bytes,
) -> None:
    handle_add(test_data, file_name, user)
    storage = cast(FakeStorageClient, container.storage_client())
    obj_key: str
    obj_key, _ = list(storage._objects.items())[0]
    assert obj_key.split("/", 1)[0] == user
    assert obj_key.rsplit(".", 1)[1] == file_name.rsplit(".", 1)[1]
