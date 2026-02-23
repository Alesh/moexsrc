import typing as t
from collections.abc import AsyncIterable
from datetime import date, datetime

import moexsrc.session
import moexsrc.tickers
import moexsrc.markets
import moexsrc.assets
import moexsrc.utils
from moexsrc.types import Period, TickerFilter, AssetFilter

__all__ = ["Asset", "Market", "Period", "Ticker"]

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


class Asset(moexsrc.assets.Asset):
    """
    Класс реализует методы для получения информации по биржевому активу адаптированные для работы с pandas.
    """

    def __init__(self, assetcode: str):
        super().__init__(moexsrc.session.ctx, assetcode)

    async def get_tickers(self, **filter: t.Unpack[TickerFilter]) -> list[Ticker]:
        tickers = list()
        for ticker_ in await moexsrc.utils.rollup(super().get_tickers(**filter)):
            ticker = Ticker(ticker_.symbol)
            ticker._desc.update(ticker_._desc)
            tickers.append(ticker)
        return tickers

    async def futoi(
        self,
        period: Period | t.Literal["5min", "1D"] = "5min",
        /,
        *,
        begin: str | date | datetime | None = None,
        end: str | date | datetime | None = None,
        latest: int | None = None,
    ) -> pd.DataFrame:
        return await dataframe(super().futoi(period, begin=begin, end=end, latest=latest))


class Market(moexsrc.markets.Market):
    """
    Класс реализует методы для получения информации по биржевому разделу адаптированные для работы с pandas.
    """

    def __init__(self, arg: str, *args: str):
        super().__init__(moexsrc.session.ctx, arg, *args)

    async def get_tickers(self, **filter: t.Unpack[TickerFilter]) -> list[Ticker]:
        tickers = list()
        for ticker_ in await moexsrc.utils.rollup(super()._get_tickers(**filter)):
            ticker = Ticker(ticker_.symbol)
            ticker._desc.update(ticker_._desc)
            tickers.append(ticker)
        return tickers

    async def get_assets(self, **filter: t.Unpack[AssetFilter]) -> list[Asset]:
        assets = list()
        for asset_ in await moexsrc.utils.rollup(super()._get_assets(**filter)):
            asset = Asset(asset_.symbol)
            asset._desc.update(asset_._desc)
            assets.append(asset)
        return assets
