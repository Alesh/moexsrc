import typing as t
from collections.abc import Sequence
from datetime import datetime, date
from enum import Enum


class TickerFilter(t.TypedDict, total=False):
    engine: str
    market: str
    boardid: str
    assetcode: str
    is_traded: bool
    isin: str


class AssetFilter(t.TypedDict, total=False):
    is_traded: bool


class Period(Enum):
    """
    Перечесление периодов руночных данных.
    """

    ONE_MINUTE = 1
    FIVE_MINUTES = 5
    TEN_MINUTES = 10
    ONE_HOUR = 60
    ONE_DAY = 24
    ONE_WEEK = 7
    ONE_MONTH = 31

    @classmethod
    def from_literal(cls, value: t.Literal["1min", "5min", "10min", "1h", "1D", "1W", "1M"] | str) -> t.Self:
        """Создает период из литерала."""
        match value:
            case "1min":
                return Period.ONE_MINUTE
            case "5min":
                return Period.FIVE_MINUTES
            case "10min":
                return Period.TEN_MINUTES
            case "1h":
                return Period.ONE_HOUR
            case "1D" | "1d":
                return Period.ONE_DAY
            case "1W" | "1w":
                return Period.ONE_WEEK
            case "1M" | "1m":
                return Period.ONE_MONTH
            case _:
                raise ValueError(f"Invalid period {value}")

    @property
    def literal(self) -> t.Literal["1min", "5min", "10min", "1h", "1D", "1W", "1M"]:
        match self:
            case Period.ONE_MINUTE:
                return "1min"
            case Period.FIVE_MINUTES:
                return "5min"
            case Period.TEN_MINUTES:
                return "10min"
            case Period.ONE_HOUR:
                return "1h"
            case Period.ONE_DAY:
                return "1D"
            case Period.ONE_WEEK:
                return "1W"
            case Period.ONE_MONTH:
                return "1M"

    @property
    def minutes(self) -> int | None:
        """Количество минут в периоде, или None если не применимо."""
        if self.value in (1, 5, 10, 60):
            return self.value
        elif self.value == 24:
            return 60 * 24
        elif self.value == 7:
            return 60 * 24 * 7
        return None


class Candle(t.TypedDict):
    secid: str
    assetcode: t.NotRequired[str]
    period: Period
    open: float
    high: float
    low: float
    close: float
    volume: float
    value: t.NotRequired[float]
    begin: date | datetime
    end: date | datetime


class FutOI(t.TypedDict):
    assetcode: str
    clgroup: t.Literal["FIZ", "YUR"]
    period: Period
    pos: float
    pos_long: float
    pos_long_num: int
    pos_short: float
    pos_short_num: int
    sectype: str
    sess_id: int
    session_date: date
    seqnum: int
    systime: datetime
    tradetime: datetime
