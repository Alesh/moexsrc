import typing as t

from moexsrc.session import SessionCtx
from moexsrc.utils import extract, rollup

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


class HasDesc(t.Protocol):
    _desc: dict[str, t.Any]


NO_SECTYPE = ("CNYRUBF", "EURRUBF", "GAZPF", "GLDRUBF", "IMOEXF", "SBERF", "USDRUBF")  # исключения для тикера FutOI


async def resolve_path(ctx: SessionCtx, hd: HasDesc, topic: str) -> str | None:
    symbol = None
    assetcode, secid = extract(hd._desc, "assetcode", "secid")
    if secid is not None:
        # Ticker
        if security := await ctx.client.get_security(secid):
            hd._desc.update(security)
        else:
            return None
        symbol = secid
    else:
        if assetcode is not None:
            # Asset
            tickers = await rollup(hd._get_tickers())
            symbol = tickers[0].symbol
            if symbol not in NO_SECTYPE:
                symbol = symbol[:2]
    match topic:
        case "candles":
            if secid:
                engine, market, boardid, secid = extract(hd._desc, "engine", "market", "boardid", "secid")
                return f"engines/{engine}/markets/{market}/boards/{boardid}/securities/{secid}/candles"
            return None
        case "futoi":
            if assetcode and symbol:
                return f"analyticalproducts/futoi/securities/{symbol}"
            return None
        case _:
            raise ValueError(f"Unknown topic: {topic}")
