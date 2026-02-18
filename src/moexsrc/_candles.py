import typing as t
from collections.abc import AsyncIterable
from datetime import date, datetime, timedelta

from moexsrc.iss_client import ISSClient
from moexsrc.types import Period


def request_candles(client: ISSClient, path: str, params: t.Dict[str, t.Any]):
    """Получает данные свечного графика."""
    return client.request(path, "candles", **params)


async def resample_candles(
    it: AsyncIterable[dict[str, t.Any]], period: Period, begin: date, end: date
) -> AsyncIterable[dict[str, t.Any]]:
    """Ресамлирует данные свечного графика."""

    def range_it(begin: date, end: date):
        minutes = period.minutes
        end_max = datetime.combine(end, datetime.max.time())
        begin = datetime.combine(begin, datetime.min.time())
        end = begin + timedelta(minutes=minutes) - timedelta(microseconds=1)
        while end < end_max:
            yield begin, end
            begin += timedelta(minutes=minutes)
            end = begin + timedelta(minutes=minutes) - timedelta(microseconds=1)

    def make_candle(accum: list[dict[str, t.Any]], begin: datetime, end: datetime) -> dict[str, t.Any]:
        accum.sort(key=lambda x: x["begin"])
        candle = dict(
            begin=begin,
            end=end,
            open=accum[0]["open"],
            high=max(item["high"] for item in accum),
            low=min(item["high"] for item in accum),
            close=accum[-1]["close"],
            volume=sum(item["volume"] for item in accum),
            value=sum(item["value"] for item in accum),
        )
        accum.clear()
        return candle

    accum = list()
    range = range_it(begin, end)
    begin, end = next(range)
    async for item in it:
        begin_item = datetime.fromisoformat(item["begin"])
        try:
            while end < begin_item:
                if accum:
                    yield make_candle(accum, begin, end)
                begin, end = next(range)
            accum.append(item)
        except StopIteration:
            break
    if accum:
        yield make_candle(accum, begin, end)


async def normalize_candles(it: AsyncIterable[dict[str, t.Any]], repr: t.Literal["it", "list"] = "list"):
    """Нормализует данные свечного графика."""

    async def async_():
        async for item in it:
            yield item

    if repr == "list":
        return [item async for item in async_()]
    return async_()
