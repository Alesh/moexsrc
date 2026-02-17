import os

import pytest


@pytest.fixture(scope="session")
def api_key():
    return os.environ.get("APIKEY")
