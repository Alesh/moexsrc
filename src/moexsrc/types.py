import typing as t
from enum import Enum


class Period(Enum):
    ONE_MINUTE = 1
    FIVE_MINUTES = 5
    TEN_MINUTES = 10
    ONE_HOUR = 60
    ONE_DAY = 24
    ONE_WEEK = 7
    ONE_MONTH = 31

    @classmethod
    def from_literal(cls, value: t.Literal["1min", "5min", "10min", "1h", "1D", "1W", "1M"]) -> t.Self:
        match value:
            case "1min":
                return Period.ONE_MINUTE
            case "5min":
                return Period.FIVE_MINUTES
            case "10min":
                return Period.TEN_MINUTES
            case "1h":
                return Period.ONE_HOUR
            case "1D":
                return Period.ONE_DAY
            case "1W":
                return Period.ONE_WEEK
            case "1M":
                return Period.ONE_MONTH
            case _:
                raise ValueError(f"Invalid period {value}")
