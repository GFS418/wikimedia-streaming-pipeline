import socket
import uuid

import pytest

from wikistream.config import Settings


def _reachable(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=1):
            return True
    except OSError:
        return False


@pytest.fixture(scope="session")
def stack():
    if not (_reachable("localhost", 9092) and _reachable("localhost", 8081)):
        pytest.skip("local stack not running (docker compose up -d)")
    return Settings()


@pytest.fixture
def run_id():
    return uuid.uuid4().hex[:8]
