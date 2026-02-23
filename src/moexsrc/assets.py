import typing as t
from collections.abc import AsyncIterator

from moexsrc.session import SessionCtx
from moexsrc.tickers import Ticker
from moexsrc.types import TickerFilter
from datetime import date, datetime, timedelta

from moexsrc._futoi import normalize_futoi, daily_futoi
from moexsrc.resolver import resolve_path, NO_SECTYPE
from moexsrc.types import Period, FutOI
from moexsrc.utils import to_date, limited, rollup, puffup, date_pair_gen


class Asset:
    """
    Класс реализует методы для получения информации по рыночному активу.
    """

    def __init__(self, ctx: SessionCtx, assetcode: str):
        self._desc = dict(assetcode=assetcode)
        self._tickers = list()
        self._ctx = ctx

    def __repr__(self) -> str:
        return f'Asset("{self.symbol}")'

    def __str__(self) -> str:
        return repr(self)

    @property
    def symbol(self) -> str:
        """Символьное представление актива."""
        return self._desc["assetcode"]

    def get_tickers(self, **filter: t.Unpack[TickerFilter]) -> AsyncIterator[Ticker]:
        """Асинхронный итератор возвращающий инструменты рынка."""
        return self._get_tickers(**filter)

    async def _get_tickers(self, **filter: t.Unpack[TickerFilter]) -> AsyncIterator[Ticker]:
        filter = dict(is_traded=1, **filter, assetcode=self._desc["assetcode"])
        if "is_traded" in filter and filter["is_traded"] and self._tickers:
            for ticker in self._tickers:
                yield ticker
        else:
            self._tickers.clear()
            path = "engines/futures/markets/forts/securities.json"
            async for short in self._ctx.client.request(path, "securities", start=-1):
                short = dict([(k.lower(), v) for k, v in short.items()], is_traded=1)
                if all(short.get(k) == v for k, v in filter.items()):
                    ticker = Ticker(self._ctx, short["secid"])
                    ticker._desc.update(short)
                    self._tickers.append(ticker)
                    yield ticker
        symbol = self._tickers[0].symbol
        self._desc.update(sectype=(symbol if symbol in NO_SECTYPE else symbol[:2]))
        # ToDo: Переделать на 'statistics/engines/futures/markets/forts/series', что бы получать также is_traded=False

    async def futoi(
        self,
        period: Period | t.Literal["5min", "1D"] = "5min",
        /,
        *,
        begin: str | date | datetime | None = None,
        end: str | date | datetime | None = None,
        latest: int | None = None,
    ) -> AsyncIterator[FutOI]:
        """
        Данные FutOI по заданным параметрам

        Args:
            period: Период свечи, по умолчанию "5min"
            begin: Начиная с какого времени выдать данные
            end: По какое времени выдать данные
            latest: Включает вывод последних 1 <= N <= 12 записей отсортированных в обратном порядке
        """
        path = await resolve_path(self._ctx, self, "futoi")
        if path is None:
            raise NotImplementedError("FutOI not implemented for this ticker")
        ticker = path.split("/")[-1].split(".")[0]
        period = period if isinstance(period, Period) else Period.from_literal(period)
        if period not in (Period.FIVE_MINUTES, Period.ONE_DAY):
            raise ValueError(f"Period {period} not implemented for this dataset")
        if latest is None:
            begin = to_date(begin)
            end = to_date(end)
        else:
            if not (1 <= latest <= 12):
                raise ValueError("Value for latest must be between 1 and 12")
            end = date.today()
            begin = end - timedelta(days=10)

        if period is Period.ONE_DAY:
            # Дневные метрики скачиваются с ендпоитов наполняющих сайт moex.com
            if latest is None:
                dates = list(d for d, _ in date_pair_gen(begin, end, 1))
            else:
                dates = list(reversed(list(d for d, _ in date_pair_gen(begin, end, 1))))
            aiter = daily_futoi(self.symbol, *dates)
        else:
            # Пагинация для ISS FutOI, одним запросом скачиваю два дня
            if latest is None:
                date_pairs = date_pair_gen(begin, end)
            else:
                date_pairs = reversed(list(date_pair_gen(begin, end)))

            async def aiter_():
                for begin_, end_ in date_pairs:
                    params = {"from": begin_.isoformat(), "till": end_.isoformat()}
                    items = await rollup(self._ctx.client.request(path, "futoi", **params, start=-1))
                    async for item in puffup(reversed(items)):
                        yield item

            aiter = aiter_()

        if latest:
            aiter = limited(aiter, latest * 2)
        extra = dict(**dict((k, v) for k, v in self._desc.items() if k in ("assetcode",)), ticker=ticker, period=period)
        async for item in aiter:
            yield normalize_futoi(**dict(item, **extra))
