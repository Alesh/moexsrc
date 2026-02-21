import typing as t
from moexsrc.session import SessionCtx
from moexsrc.tickers import Ticker
from moexsrc.types import TickerFilter
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

    async def get_tickers(self, **filter: t.Unpack[TickerFilter]):
        """Асинхронный итератор возаращающий инструменты рынка."""
        engine, market, boardid = extract(self._desc, "engine", "market", "boardid")
        path = f"engines/{engine}/markets/{market}/boards/{boardid}/securities.json"
        async for short in self._ctx.client.request(path, "securities", start=-1):
            short = dict((k.lower(), v) for k, v in short.items())
            if not filter or all(short.get(k) == v for k, v in filter.items()):
                ticker = Ticker(self._ctx, short["secid"])
                ticker._desc.update(short)
                yield ticker


ALIASES = {
    ("stock", "shares", "TQBR"): ["eq", "stock", "shares"],
    ("currency", "selt", "CETS"): ["fx", "currency", "selt", "forex"],
    ("futures", "forts", "RFUD"): ["fo", "futures", "forts"],
}


def resolve_desc(engine: str, market: str, boardid: str) -> dict[str, t.Any]:
    candidate = None
    for engine_, market_, boardid_ in ALIASES.keys():
        if engine == engine and market_ == market:
            candidate = dict(engine=engine_, market=market_, boardid=boardid_)
            if boardid == boardid_:
                return candidate
    if candidate is not None:
        return candidate
    raise ValueError(f"Unrecognized market path: {market}")


def resolve_alias(alias: str) -> dict[str, t.Any]:
    alias_ = alias.lower()
    for result, aliases in ALIASES.items():
        if alias_ in aliases:
            return dict(zip(("engine", "market", "boardid"), result))
    raise ValueError(f"Unrecognized  market alias: {alias}")
