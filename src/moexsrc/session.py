import typing as t

import moexsrc.issclient

TOKEN: str | None = None
BASE_URL: str | None = None
REQUEST_TIMEOUT = 60
IDLE_TIMEOUT = 0.1

_current = dict()


class SessionCtx(t.NamedTuple):
    client: moexsrc.issclient.ISSClient


def __getattr__(name):
    match name:
        case "ctx":
            if "client" not in _current:
                _current["client"] = moexsrc.issclient.ISSClient(TOKEN, BASE_URL)
            return SessionCtx(**_current)
        case _:
            raise AttributeError(f"module '{__name__}' has no attribute '{name}'")


class Session:
    """
    Класс реализует сессию подключения к источнику данных
    """

    def __init__(
        self, token: str | None = None, base_url: str | None = None, /, request_timeout: float = 60.0, idle_timeout=0.1
    ) -> None:
        self._client_kwargs = dict(
            token=token or TOKEN,
            base_url=base_url or BASE_URL,
            request_timeout=request_timeout,
            idle_timeout=idle_timeout,
        )

    def __enter__(self):
        return SessionCtx(
            client=moexsrc.issclient.ISSClient(**self._client_kwargs),
        )

    def __exit__(self, *exc_info):
        return False
