import typing as t
from collections.abc import AsyncIterator
from datetime import date, datetime

from moexsrc._candles import resample_candle, normalize_candles
from moexsrc.resolver import resolve_path
from moexsrc.session import SessionCtx
from moexsrc.types import Period, Candle
from moexsrc.utils import to_datetime, to_date, limited, rollup, puffup


class Ticker:
    """
    Класс реализует методы для получения информации по рыночному инструменту.
    """

    def __init__(self, ctx: SessionCtx, secid: str):
        self._desc = dict(secid=secid.upper())
        self._ctx = ctx

    def __repr__(self) -> str:
        return f'Ticker("{self.symbol}")'

    def __str__(self) -> str:
        return repr(self)

    @property
    def symbol(self) -> str:
        """Символьное представление инструмента."""
        return self._desc["secid"]

    async def candles(
        self,
        period: Period | t.Literal["1min", "5min", "10min", "1h", "1D", "1W", "1M"] = "10min",
        /,
        *,
        begin: str | date | datetime | None = None,
        end: str | date | datetime | None = None,
        latest: int | None = None,
    ) -> AsyncIterator[Candle]:
        """
        Данные для "Свечного графика" по заданным параметрам

        Args:
            period: Период свечи, по умолчанию "10min"
            begin: Начиная с какого времени выдать данные
            end: По какое времени выдать данные
            latest: Включает вывод последних 1 <= N <= 12 записей отсортированных в обратном порядке
        """
        path = await resolve_path(self._ctx, self, "candles")
        if path is None:
            raise NotImplementedError("Candles not implemented for this ticker")
        period = period if isinstance(period, Period) else Period.from_literal(period)
        params: dict[str, t.Any] = dict(interval=period.value)
        if latest is None:
            limit = 0
            if period.minutes:
                begin = to_datetime(begin, "begin")
                end = to_datetime(end, "end")
            else:
                begin = to_date(begin)
                end = to_date(end)
            params.update({"from": begin.isoformat(), "till": end.isoformat()})
        else:
            if not (1 <= latest <= 12):
                raise ValueError("Value for latest must be between 1 and 12")
            limit = latest
            params["iss.reverse"] = "true"
        if period is Period.FIVE_MINUTES:
            params["interval"] = 1

        extra = dict((k, v) for k, v in self._desc.items() if k in ("assetcode", "secid"))
        aiter_ = normalize_candles(self._ctx.client.request(path, "candles", **params), **extra, period=period)
        if period is Period.FIVE_MINUTES:
            if latest is not None:
                candles = await rollup(limited(aiter_, (limit + 2) * 5))
                aiter_ = puffup(reversed(candles))
                aiter_ = resample_candle(aiter_, period, candles[0]["begin"].date(), candles[0]["end"].date())
                aiter_ = puffup(reversed(await rollup(aiter_)))
            else:
                aiter_ = resample_candle(aiter_, period, begin, end)
        if limit:
            aiter_ = limited(aiter_, limit)

        async for item in aiter_:
            yield item
