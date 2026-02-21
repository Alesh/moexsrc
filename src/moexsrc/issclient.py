import asyncio
import json
import logging
import typing as t
from collections.abc import AsyncIterator

import httpx


class ISSClientError(Exception):
    """Ошибка в запросе данных от ISS."""


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
        idle_timeout=0.01,
    ):
        options: dict[str, t.Any] = dict(timeout=request_timeout)
        if api_key is not None:
            options["base_url"] = base_url or "https://apim.moex.com/iss"
            options.setdefault("headers", []).append(("Authorization", f"Bearer {api_key}"))
        else:
            options["base_url"] = base_url or "https://iss.moex.com/iss"
        self._client = httpx.AsyncClient(**options)
        self.__idle_timeout = idle_timeout

    @property
    def idle_timeout(self) -> float:
        """Тайм-аут между HTTP запросами."""
        return self.__idle_timeout

    async def request(
        self,
        path: str,
        section: str | None = None,
        deserializer: t.Callable[[dict[str, t.Any], str], list[dict[str, t.Any]]] | None = None,
        continuer: t.Callable[[dict[str, t.Any], dict[str, t.Any], str], dict[str, t.Any]] | None = None,
        **parameters: t.Any,
    ) -> AsyncIterator[dict[str, t.Any]]:
        """
        Запрос данных.

        Args:
            path: URI запроса без префикса '/iss'.
            section: Какую секцию запроса следует считать.
            deserializer: Метод десериализующий данные ответа в список словарей.
            continuer: Метод возвращает словарь параметров запроса следующей страницы, или `None` для прерывания.
            parameters: Словарь параметров запроса. Если не переопределен параметер `continuer`, `start=-1` выведет
                        только первую страницу данных.
        Returns:
            Асинхронный итератор возвращающий результат запроса.
        """

        def default_deserializer(data: dict[str, t.Any], section: str) -> list[dict[str, t.Any]]:
            data = data[section]
            if "error" in data:
                raise ISSClientError(data["error"])
            elif "ERROR_MESSAGE" in data["columns"]:
                message = data["data"][0][0]
                if "Free users can't receive data" in message:
                    logging.debug(message)
                    return []
                raise ISSClientError(message)
            return [dict(zip(data["columns"], row)) for row in data["data"]]

        def default_continuer(
            params: dict[str, t.Any], data: dict[str, t.Any], section: str
        ) -> dict[str, t.Any] | None:
            if section != "*":
                data = data[section]["data"]
                start = params.get("start", 0)
                if len(data) > 0 and start >= 0:
                    return dict(start=start + len(data))
            return None

        path = path + ".json" if not path.endswith(".json") else path
        params = dict(parameters, **{"iss.meta": "off"})
        continuer = continuer or default_continuer

        if deserializer is None:
            if section is None:
                section = path.split("/")[-1].split(".")[0]
            deserializer = default_deserializer

        def process_response(resp: httpx.Response) -> tuple[list[dict[str, t.Any]], dict[str, t.Any] | None]:
            if resp.is_success:
                if resp.headers.get("content-type", "").startswith("application/json"):
                    if data := json.loads(resp.text):
                        result = deserializer(data, section)
                        if continue_params := continuer(params, data, section):
                            params.update(continue_params)
                            return result, params
                        return result, None
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
                await asyncio.sleep(self.__idle_timeout)
                continue
            break

    async def get_security(self, secid: str) -> dict[str, t.Any] | None:
        """
        Возращает информацию об инструменте, или `None` если не найдено.

        Args:
            secid: Код инструмента.

        Returns:
            Словарь описывающий инструмент.
        """

        def deserializer(data: dict[str, t.Any], _: str) -> list[dict[str, t.Any]]:
            result = list()
            description = [dict(zip(data["description"]["columns"], row)) for row in data["description"]["data"]]
            boards = list(
                filter(
                    lambda item: item["is_primary"],
                    [dict(zip(data["boards"]["columns"], row)) for row in data["boards"]["data"]],
                )
            )
            if boards:
                data = dict(
                    (item["name"].lower(), int(item["value"]) if item["value"].isnumeric() else item["value"])
                    for item in description
                )
                if data or boards[0]:
                    result.append(dict(data, **boards[0]))
            return result

        if found := [s async for s in self.request(f"securities/{secid}", "*", deserializer, start=-1)]:
            return found[0]
        return None

    async def get_market_securities(
        self, engine: str, market: str, board: str | None = None, only_active=True
    ) -> AsyncIterator[dict[str, t.Any]]:
        """
        Возвращает информацию об инструментах рынка.

        Args:
            engine: Рынок
            market: Раздел рынка
            board: Торговая площадка
            only_active: Флаг вывода только активных (торгуемых)

        Returns:
            Асинхронный итератор возвращающий краткую информацию об инструментах рынка.
        """

        def deserializer(data: dict[str, t.Any], _: str) -> list[dict[str, t.Any]]:
            result = list()
            data = data["securities"]
            for row in data["data"]:
                row = dict(zip(data["columns"], row))
                if board is None or row["primary_boardid"] == board:
                    result.append(dict(row, engine=engine, market=market, boardid=row["primary_boardid"]))
            return result

        def continuer(params: dict[str, t.Any], data: dict[str, t.Any], _: str) -> dict[str, t.Any] | None:
            data = data["securities"]["data"]
            start = params.get("start", 0)
            if len(data) > 0:
                return dict(start=start + len(data))
            elif params.get("is_trading", 1) and not only_active:
                return dict(is_trading=0)
            return None

        params = dict(
            engine=engine, market=market, group_by="group", group_by_filter=f"{engine}_{market}", is_trading=1
        )
        async for security in self.request("securities", None, deserializer, continuer, **params):
            yield security
