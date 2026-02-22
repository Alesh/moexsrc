import typing as t
from collections.abc import AsyncIterable
from datetime import date, datetime

import moexsrc.session
import moexsrc.tickers
import moexsrc.utils
from moexsrc.types import Period, TickerFilter

__all__ = ["Market", "Period", "Ticker"]

try:
    import pandas as pd
except ImportError:
    raise ImportError("You must install pandas to use module `moexsrc.dataframes`.")


async def dataframe(it: AsyncIterable[t.Any]) -> pd.DataFrame:
    """ "Сворачивает" асинхронный итератор в `pandas.DataFrame`."""
    return pd.DataFrame.from_records(await moexsrc.utils.rollup(it))


class Ticker(moexsrc.tickers.Ticker):
    """
    Класс реализует методы для получения информации по рыночному инструменту адаптированные для работы с pandas.
    """

    def __init__(self, secid: str):
        super().__init__(moexsrc.session.ctx, secid)

    async def candles(
        self,
        period: Period | t.Literal["1min", "5min", "10min", "1h", "1D", "1W", "1M"] = "10min",
        /,
        *,
        begin: str | date | datetime | None = None,
        end: str | date | datetime | None = None,
        latest: int | None = None,
        offset: int | None = None,
        limit: int | None = None,
    ) -> pd.DataFrame:
        return await dataframe(super().candles(period, begin=begin, end=end, latest=latest))


class Market(moexsrc.markets.Market):
    """
    Класс реализует методы для получения информации по биржевому разделу адаптированные для работы с pandas.
    """

    def __init__(self, arg: str, *args: str):
        super().__init__(moexsrc.session.ctx, arg, *args)

    async def get_tickers(self, **filter: t.Unpack[TickerFilter]) -> list[Ticker]:
        tickers = list()
        for ticker_ in await moexsrc.utils.rollup(super().get_tickers(**filter)):
            ticker = Ticker(ticker_.symbol)
            ticker._desc.update(ticker_._desc)
            tickers.append(ticker)
        return tickers
