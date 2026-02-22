import typing as t
from collections.abc import AsyncIterable, AsyncIterator
from datetime import datetime, date, timedelta

from moexsrc.types import Candle, Period
from moexsrc.utils import to_datetime


def normalize_candle(**data: t.Any) -> Candle:
    """Нормализует данные свечи."""
    match data:
        case {
            "begin": begin,
            "end": end,
            "open": open,
            "high": high,
            "low": low,
            "close": close,
            "volume": volume,
            **kwargs,
        }:
            if "value" in kwargs:
                kwargs["value"] = float(round(kwargs["value"], 0))
            begin = datetime.fromisoformat(begin) if isinstance(begin, str) else begin
            end = datetime.fromisoformat(end) if isinstance(end, str) else end
            minutes = kwargs["period"].minutes
            if minutes not in (1, 5, 10, 60):
                begin = begin.date()
                end = end.date()
            else:
                end = (begin + timedelta(minutes=minutes)) - timedelta(seconds=1)
            return Candle(
                open=float(open),
                high=float(high),
                low=float(low),
                close=float(close),
                volume=int(volume),
                begin=begin,
                end=end,
                **kwargs,
            )
        case _:
            raise ValueError(f"Invalid candle data: {data}")


async def normalize_candles(aiter_: AsyncIterable[dict[str, t.Any]], **extra) -> AsyncIterator[Candle]:
    """Нормализует данные свечей."""
    async for item in aiter_:
        yield normalize_candle(**item, **extra)


async def resample_candle(
    aiter_: AsyncIterable[Candle], period: Period, begin: date | datetime, end: date | datetime
) -> AsyncIterator[Candle]:
    """Ресемлирует данные свечного графика."""
    extra = dict()
    minutes = period.minutes
    if minutes is None:
        raise ValueError("This is dataset cannot be resampled")

    def range_it(begin: date, end: date):
        begin = to_datetime(begin)
        end_max = to_datetime(end, "end")
        end = begin + timedelta(minutes=minutes) - timedelta(microseconds=1)
        while end < end_max:
            yield begin, end
            begin += timedelta(minutes=minutes)
            end = (begin + timedelta(minutes=minutes)) - timedelta(microseconds=1)

    def make_candle(accum: list[dict[str, t.Any]], begin: datetime, end: datetime) -> Candle:
        if len(extra) == 0:
            extra.update(
                (
                    (k, v)
                    for k, v in accum[0].items()
                    if k not in ("begin", "end", "open", "high", "low", "close", "volume", "value", "period")
                ),
                period=period,
            )
        accum.sort(key=lambda x: x["begin"])
        candle = normalize_candle(
            begin=begin,
            end=end,
            open=accum[0]["open"],
            high=max(item["high"] for item in accum),
            low=min(item["high"] for item in accum),
            close=accum[-1]["close"],
            volume=sum(item["volume"] for item in accum),
            value=sum(item["value"] for item in accum),
            **extra,
        )
        accum.clear()
        return candle

    accum = list()
    range = range_it(begin, end)
    begin, end = next(range)
    async for item in aiter_:
        begin_item = item["begin"]
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
