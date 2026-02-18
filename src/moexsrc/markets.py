import typing as t

from moexsrc._futoi import normalize_futoi, request_futoi
from moexsrc.iss_client import ISSClient
from moexsrc.types import Period
import datetime

from moexsrc.utils import normalize_market_params


class Market:
    """ """

    def __init__(self, client: ISSClient, arg: str, *args: str):
        self._path = self.resolve_alias(arg) if not args else self.resolve_path(arg, *args)
        self.client = client

    ALIASES = {
        ("stock", "shares", "TQBR"): ["stock", "shares", "eq"],
        ("currency", "selt", "CETS"): ["currency", "selt", "forex", "fx"],
        ("futures", "forts", "RFUD"): ["futures", "forts", "fo"],
    }

    @staticmethod
    def resolve_path(engine: str, market: str, board: str) -> tuple[str, str, str]:
        candidate = None
        for engine_, market_, board_ in Market.ALIASES.keys():
            if engine == engine and market_ == market:
                candidate = engine_, market_, board_
                if board == board_:
                    return engine_, market_, board_
        if candidate is not None:
            return candidate
        raise ValueError(f"Unrecognized market path: {market}")

    @staticmethod
    def resolve_alias(alias: str) -> tuple[str, str, str]:
        alias_ = alias.lower()
        for result, aliases in Market.ALIASES.items():
            if alias_ in aliases:
                return result
        raise ValueError(f"Unrecognized  market alias: {alias}")

    async def get_path(self, topic: t.Literal["candles", "algopack", "futoi"]) -> str:
        """"""
        engine, market, board = self._path
        match topic:
            case "candles":
                return f"engines/{engine}/markets/{market}/boards/{board}/securities"
            case "algopack":
                raise NotImplementedError
            case "futoi":
                if self._path != ("futures", "forts", "RFUD"):
                    raise NotImplementedError("FUTOI not implemented for this market")
                return f"analyticalproducts/futoi/securities"
            case _:
                raise ValueError(f"Unknown topic: {topic}")

    async def get_securities(self, *fields, delisted=False, fullinfo=False) -> list[dict[str, t.Any]]:
        fields = fields or ("engine", "market", "board", "secid")
        return [
            dict((key, value) for key, value in item.items() if key in fields)
            async for item in self.client.securities(*self._path, delisted=delisted, fullinfo=fullinfo)
        ]

    async def futoi(
        self,
        period: Period | t.Literal["5min", "1D"] = "5min",
        /,
        *,
        date: datetime.date | str = None,
        latest: int | None = None,
    ):
        """
        Метрики `FUTOI` по заданным параметрам.
        """
        path = await self.get_path("futoi")
        period, date = normalize_market_params(period, date)
        if not period in (
            Period.FIVE_MINUTES,
            Period.ONE_DAY,
        ):
            raise ValueError(f"Period {period} not implemented for this dataset")
        params = {"date": date.isoformat(), "interval": period.value}
        return await normalize_futoi(request_futoi(self.client, path, params))
