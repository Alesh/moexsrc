import asyncio
import json
import typing as t
from collections.abc import AsyncGenerator, Generator, Awaitable
from time import sleep

import httpx


class ISSClientError(Exception):
    """
    Something went wrong when trying to access the ISS API.
    """


class ISSClient:
    """
    ISS клиент.
    """

    def __init__(
        self,
        api_key: str | None = None,
        base_url: str | None = None,
        /,
        *,
        request_timeout=60,
        idle_timeout=0.3,
    ):
        options: dict[str, t.Any] = dict(timeout=request_timeout)
        if api_key is not None:
            options["base_url"] = base_url or "https://apim.moex.com/iss"
            options.setdefault("headers", []).append(("Authorization", f"Bearer {api_key}"))
        else:
            options["base_url"] = base_url or "https://iss.moex.com/iss"
        self._client = httpx.AsyncClient(**options)
        self._idle_timeout = idle_timeout

    async def request(
        self,
        path: str,
        section: str | None = None,
        deserializer: t.Callable[[dict[str, t.Any]], list[dict[str, t.Any]]] | None = None,
        continuer: t.Callable[[dict[str, t.Any], list[dict[str, t.Any]]], dict[str, t.Any]] | None = None,
        **kwargs: t.Any,
    ) -> AsyncGenerator[dict[str, t.Any]]:
        """
        Запрос на получение данных.
        """

        def default_deserializer(data: dict[str, t.Any]) -> list[dict[str, t.Any]]:
            if "error" in data:
                raise ISSClientError(data["error"])
            elif "ERROR_MESSAGE" in data["columns"]:
                raise ISSClientError(data["data"][0][0])
            return [dict(zip(data["columns"], row)) for row in data["data"]]

        def default_continuer(params: dict[str, t.Any], data: list[dict[str, t.Any]]) -> dict[str, t.Any] | None:
            start = params.get("start", 0)
            if len(data) > 0 and start >= 0:
                return dict(start=start + len(data))
            return None

        path = path + ".json" if not path.endswith(".json") else path
        params = dict(kwargs, **{"iss.meta": "off"})
        continuer = continuer or default_continuer

        if deserializer is None:
            if section is None:
                section = path.split("/")[-1].split(".")[0]
            deserializer = default_deserializer

        def process_response(resp: httpx.Response) -> tuple[list[dict[str, t.Any]], dict[str, t.Any] | None]:
            if resp.is_success:
                if resp.headers.get("content-type", "").startswith("application/json"):
                    if data := json.loads(resp.text):
                        data = deserializer(data[section] if section != "*" else data)
                        if continue_params := continuer(params, data):
                            params.update(continue_params)
                            return data, params
                        return data, None
                else:
                    resp.status_code = 403
                resp.status_code = 400
            resp.raise_for_status()
            raise RuntimeError("Unreachable")

        while True:
            resp = await self._client.get(
                path, params=dict((key, value) for key, value in params.items() if not (key == "start" and value < 0))
            )
            data, params = process_response(resp)
            for rec in data:
                yield rec
            if params:
                await asyncio.sleep(self._idle_timeout)
                continue
            break

    async def security(self, secid: str) -> dict[str, t.Any] | None:
        """Возращает информациб о конкретном инструменте, или `None` если не найдено."""

        def deserializer(data: dict[str, t.Any]) -> list[dict[str, t.Any]]:
            description = [dict(zip(data["description"]["columns"], row)) for row in data["description"]["data"]]
            boards = [dict(zip(data["boards"]["columns"], row)) for row in data["boards"]["data"]]
            data = dict((item["name"].lower(), item["value"]) for item in description)
            data.update([b for b in boards if b["is_primary"]][0])
            return [data]

        if found := [s async for s in self.request(f"securities/{secid}", "*", deserializer, start=-1)]:
            data = found[0]
            path = f"engines/{data['engine']}/markets/{data['market']}/boards/{data['boardid']}/securities/{secid}"
            if found := [item async for item in self.request(path, "securities", start=-1)]:
                data.update((key.lower(), value) for key, value in found[0].items())
            return dict((key, int(value) if key in ("lotsize", "decimals") else value) for key, value in data.items())
        else:
            raise LookupError(f"Not found security with code: {secid}")

    async def securities(self, engine: str, market: str, board: str = None):
        """Возвращает информацию об инструментах для заданных параметров."""
        params = dict(start=-1)
        if board is None:
            params["primary_board"] = 1
            path = f"engines/{engine}/markets/{market}/securities"
        else:
            path = f"engines/{engine}/markets/{market}/boards/{board}/securities"
        return [
            dict(
                dict((("LOTSIZE" if key == "LOTVOLUME" else key).lower(), value) for key, value in item.items()),
                is_traded=True,
                engine=engine,
                market=market,
            )
            async for item in self.request(path, "securities", **params)
        ]
