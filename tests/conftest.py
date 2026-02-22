import os

import pytest
from moexsrc.issclient import ISSClient


@pytest.fixture
def token():
    return os.environ.get("APIKEY")


@pytest.fixture
def client(token):
    return ISSClient(token)


@pytest.fixture
def check_fields(client):
    #
    def check_fields_(security, desc):
        if isinstance(desc, dict):
            requires = list(desc.keys())
        else:
            requires = desc
        founds = [field in security for field in requires]
        assert all(founds), (
            f"Required fields: {[requires[N] for N in range(len(requires)) if not founds[N]]} has not been set"
        )
        if requires != desc:
            checks = [isinstance(security[key], desc[key]) for key in requires]
            assert all(checks), (
                f"Required fields: {[requires[N] for N in range(len(requires)) if not checks[N]]} has been wrong type"
            )
        return True

    return check_fields_
