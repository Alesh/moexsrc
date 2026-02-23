import typing as t
from collections.abc import AsyncIterator

from moexsrc.assets import Asset
from moexsrc.resolver import ALIASES, resolve_desc, resolve_alias, NO_SECTYPE
from moexsrc.session import SessionCtx
from moexsrc.tickers import Ticker
from moexsrc.types import TickerFilter, AssetFilter
from moexsrc.utils import extract


class Market:
    """
    Класс реализует методы для получения информации по биржевому разделу.
    """

    def __init__(self, ctx: SessionCtx, arg: str, *args: str):
        if not args:
            if "/" in arg:
                self._desc = resolve_desc(*arg.split("/"))
            else:
                self._desc = resolve_alias(arg)
        else:
            self._desc = resolve_desc(arg, *args)
        self._ctx = ctx

    def __repr__(self) -> str:
        symbol = ALIASES[extract(self._desc, "engine", "market", "boardid")][0].upper()
        return f'Market("{symbol}")'

    def __str__(self) -> str:
        return repr(self)

    def get_tickers(self, **filter: t.Unpack[TickerFilter]) -> AsyncIterator[Ticker]:
        """Асинхронный итератор возвращающий инструменты рынка."""
        return self._get_tickers(**filter)

    def get_assets(self, *assetcodes: str, **filter: t.Unpack[AssetFilter]) -> AsyncIterator[Asset]:
        """Асинхронный итератор возвращающий активы контрактов срочного рынка."""
        return self._get_assets(*assetcodes, **filter)

    async def _get_tickers(self, **filter: t.Unpack[TickerFilter]) -> AsyncIterator[Ticker]:
        engine, market, boardid = extract(self._desc, "engine", "market", "boardid")
        path = f"engines/{engine}/markets/{market}/boards/{boardid}/securities.json"
        async for short in self._ctx.client.request(path, "securities", start=-1):
            short = dict((k.lower(), v) for k, v in short.items())
            if not filter or all(short.get(k) == v for k, v in filter.items()):
                ticker = Ticker(self._ctx, short["secid"])
                ticker._desc.update(short)
                yield ticker

    async def _get_assets(self, *assetcodes: str, **filter: t.Unpack[AssetFilter]) -> AsyncIterator[Asset]:
        assets: dict[str, list[Ticker]] = dict()
        engine, market, boardid = extract(self._desc, "engine", "market", "boardid")
        if engine != "futures":
            raise NotImplementedError("This method is not implemented for this market.")

        async for ticker in self._get_tickers(**filter):
            if not assetcodes or ticker._desc["assetcode"] in assetcodes:
                assets.setdefault(ticker._desc["assetcode"], []).append(ticker)

        for assetcode, tickers in assets.items():
            asset = Asset(self._ctx, assetcode)
            symbol = tickers[0].symbol
            asset._desc.update(engine=engine, sectype=(symbol if symbol in NO_SECTYPE else symbol[:2]))
            asset._tickers = tickers
            yield asset
