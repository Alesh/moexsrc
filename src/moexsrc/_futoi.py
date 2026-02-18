import json
import typing as t
from collections.abc import AsyncIterable
from datetime import datetime, timedelta, date

import httpx

from moexsrc.iss_client import ISSClient


def request_futoi(client: ISSClient, path: str, params: t.Dict[str, t.Any]):
    """Получает данные метрик FUTOI."""
    if not path.endswith("securities"):
        # Ticker
        interval = params.pop("interval")
        if interval == 24:
            return request_futoi_daily(client, path, params)
        return request_futoi_5min(client, path, params)
    # Market
    return request_futoi_market(client, path, params)


async def request_futoi_market(client: ISSClient, path: str, params: t.Dict[str, t.Any]):
    """Получает данные метрик FUTOI по рынку."""
    # ToDo: Пока не починена пагинация ISS/FUTOI; получаю данные по каждому тикеру отдельно
    date = params.pop("date")
    params["from"] = date
    params["till"] = date
    interval = params.pop("interval")
    for security in await client.securities("futures", "forts"):
        sectype = security["secid"]
        if sectype not in ("CNYRUBF", "EURRUBF", "GAZPF", "GLDRUBF", "IMOEXF", "SBERF", "USDRUBF"):
            sectype = security["sectype"]
        path_ = f"analyticalproducts/futoi/securities/{sectype}"
        if interval == 24:
            async for item in request_futoi_daily(client, path_, {**params}):
                yield item
        else:
            async for item in request_futoi_5min(client, path_, {**params}):
                yield item


async def normalize_futoi(it: AsyncIterable[dict[str, t.Any]], repr: t.Literal["it", "list"] = "list"):
    """Нормализует выходные данные метрики FUTOI."""

    async def async_():
        async for item in it:
            yield item

    if repr == "list":
        return [item async for item in async_()]
    return async_()


async def request_futoi_5min(client: ISSClient, path: str, params: t.Dict[str, t.Any]):
    """Получает пятиминутные метрик FUTOI."""
    # ToDo: Пока не починена пагинация ISS/FUTOI; получаю данные по два дня за раз
    begin, end = date.fromisoformat(params.pop("from")), date.fromisoformat(params.pop("till"))

    def continuer(_0: dict[str, t.Any], _1: list[dict[str, t.Any]]) -> dict[str, t.Any] | None:
        return None  # ToDo: Пока не починена пагинация ISS/FUTOI; получаю только первую страницу

    def params_it():
        for begin_ in [begin + timedelta(days=N) for N in range(0, (end - begin).days + 2, 2)]:
            end_ = begin_ + timedelta(days=1)
            if end_ >= end:
                end_ = end
            yield {"from": begin_.isoformat(), "till": end_.isoformat()}
            if end_ == end:
                break

    for params in params_it():
        async for row in client.request(path, "futoi", continuer=continuer, **params):
            yield row


def convert_futoi_from_site(data: dict[str, t.Any]) -> list[dict[str, t.Any]]:
    """Конвертирует представление данных FUTOI с сайта."""
    tradedate = datetime.strptime(data["clients"]["Date"], "%d.%m.%Y").date().isoformat()
    systime = datetime.now().isoformat(" ", "seconds")
    to_int = lambda value: int(float(value.replace(",", ".").replace("\xa0", "")))
    fiz_pos_long = to_int(data["contracts"]["PhysicalLong"])
    fiz_pos_shor = to_int(data["contracts"]["PhysicalShort"])
    yur_pos_long = to_int(data["contracts"]["JuridicalLong"])
    yur_pos_shor = to_int(data["contracts"]["JuridicalShort"])
    return [
        dict(
            clgrpup="FIZ",
            tradedate=tradedate,
            tradetime="19:00:00",
            systime=systime,
            pos=fiz_pos_long - fiz_pos_shor,
            pos_long=fiz_pos_long,
            pos_short=-fiz_pos_shor,
            pos_long_num=to_int(data["clients"]["PhysicalLong"]),
            pos_short_num=to_int(data["clients"]["PhysicalShort"]),
        ),
        dict(
            clgrpup="YUR",
            tradedate=tradedate,
            tradetime="19:00:00",
            systime=systime,
            pos=yur_pos_long - yur_pos_shor,
            pos_long=yur_pos_long,
            pos_short=-yur_pos_shor,
            pos_long_num=to_int(data["clients"]["JuridicalLong"]),
            pos_short_num=to_int(data["clients"]["JuridicalShort"]),
        ),
    ]


async def request_futoi_from_site(client: httpx.AsyncClient, date_: str, assetcode: str):
    """Получает данные метрик FUTOI с сайта."""
    result = dict()
    url = f"https://www.moex.com/api/contract/OpenOptionService/{date.fromisoformat(date_)}/F/{assetcode}/json"
    resp = await client.get(url)
    if not resp.is_success:
        resp.raise_for_status()
    if not resp.headers.get("content-type", "").startswith("application/json"):
        resp.status_code = 400
        resp.raise_for_status()
    line_types = ["contracts", "daily_change", "pct_change", "clients"]
    if data := json.loads(resp.text):
        for N in range(len(data)):
            result[line_types[N]] = data[N]
    return convert_futoi_from_site(result)


async def request_futoi_daily(client: ISSClient, path: str, params: t.Dict[str, t.Any]):
    """Получает дневные метрики FUTOI."""
    # ToDo: Конечных данных за день нет в ISS/FUTOI; получаю данные с сайта moex.com по каждому дню и тикеру
    securities = dict()
    begin, end = date.fromisoformat(params.pop("from")), date.fromisoformat(params.pop("till"))

    if path.endswith("/securities"):
        raise NotImplementedError
    else:
        if security := await client.security(path.split("/")[-1]):
            securities[security["secid"]] = dict(
                (key, value)
                for key, value in security.items()
                if key in ("assetcode", "lotsize", "decimals", "sectype")
            )
            assert securities
        else:
            raise LookupError(f"Security for {path.split('/')[-1]} not found")

    def params_it():
        for begin_ in [begin + timedelta(days=N) for N in range(0, (end - begin).days + 1)]:
            end_ = begin_ + timedelta(days=1)
            if end_ >= end:
                end_ = end
            for seccode, security in securities.items():
                yield {**security, seccode: seccode, "from": begin_.isoformat(), "till": end_.isoformat()}
            if end_ == end:
                break

    for params in params_it():
        for item in await request_futoi_from_site(client._client, params["from"], params["assetcode"]):
            yield dict(item, sess_id=0, seqnum=0, ticker=params["sectype"], trade_session_date=item["tradedate"])
