import typing as t


class TickerFilter(t.TypedDict, total=False):
    engine: str
    market: str
    boardid: str
    assetcode: str
    is_traded: bool
    isin: str

