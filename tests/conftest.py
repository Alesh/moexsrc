import os

import pytest

from moexsrc.iss_client import ISSClient


@pytest.fixture(scope="session")
def api_key():
    return os.environ.get("APIKEY")


@pytest.fixture(scope="session")
def client(api_key):
    return ISSClient(api_key)
