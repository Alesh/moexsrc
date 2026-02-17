import typing as t
from collections.abc import AsyncIterable

from moexsrc.iss_client import ISSClient


def request_candles(client: ISSClient, path: str, params: t.Dict[str, t.Any]):
    """"""
    return client.request(path, "candles", **params)


async def normalize_candles(it: AsyncIterable[dict[str, t.Any]], repr: t.Literal["it", "list"] = "list"):
    """"""

    async def async_():
        async for item in it:
            yield item

    if repr == "list":
        return [item async for item in async_()]
    return async_()
