import datetime
import typing as t

from moexsrc._candles import request_candles, normalize_candles
from moexsrc._futoi import normalize_futoi, request_futoi
from moexsrc.iss_client import ISSClient
from moexsrc.types import Period
from moexsrc.utils import normalize_tickers_params


class Ticker:
    """
    Контекст тикера
    """

    def __init__(self, client: ISSClient, secid: str):
        self._path = (None, None, None, secid)
        self._client = client

    _securities: dict[tuple[str, str, str], list[dict[str, dict[str, t.Any]]]] = dict()

    async def get_path(self, topic: t.Literal["candles", "algopack", "futoi"]) -> str:
        """"""
        if self._path[0] is None:
            secid = self._path[3].upper()
            if found := [s for s in sum(list(self._securities.values()), []) if s["secid"] == secid]:
                self._path = found[0]["engine"], found[0]["market"], found[0]["boardid"], secid
            elif found := await self._client.security(secid):
                engine, market, boardid = found["engine"], found["market"], found["boardid"]
                self._securities.setdefault((engine, market, boardid), []).append(found)
                self._path = engine, market, boardid, secid
            else:
                raise LookupError(f"Securities {secid} not found")

        engine, market, board, secid = self._path
        match topic:
            case "candles":
                return f"engines/{engine}/markets/{market}/boards/{board}/securities/{secid}/candles"
            case "algopack":
                raise NotImplementedError
            case "futoi":
                if self._path[:3] != ("futures", "forts", "RFUD"):
                    raise NotImplementedError("FUTOI not implemented for this ticker")
                sectype = secid
                if secid not in ("CNYRUBF", "EURRUBF", "GAZPF", "GLDRUBF", "IMOEXF", "SBERF", "USDRUBF"):
                    sectype = [s for s in self._securities[(engine, market, board)] if s["secid"] == secid][0][
                        "sectype"
                    ]
                return f"analyticalproducts/futoi/securities/{sectype}"

            case _:
                raise ValueError(f"Unknown topic: {topic}")

    async def candles(
        self,
        period: Period | t.Literal["1min", "10min", "1h", "1D", "1W", "1M"] = "10min",
        /,
        *,
        begin: str | datetime.date | None = None,
        end: str | datetime.date | None = None,
    ):
        """
        Данные для "Свечного графика" по заданным параметрам
        """
        period, begin, end = normalize_tickers_params(period, begin, end)
        if not period in (
            Period.ONE_MINUTE,
            Period.TEN_MINUTES,
            Period.ONE_HOUR,
            Period.ONE_DAY,
            Period.ONE_WEEK,
            Period.ONE_MONTH,
        ):
            raise ValueError(f"Period {period} not implemented for this dataset")

        path = await self.get_path("candles")
        params = {"from": begin.isoformat(), "till": end.isoformat(), "interval": period.value}
        return await normalize_candles(request_candles(self._client, path, params))

    async def futoi(
        self,
        period: Period | t.Literal["5min", "1D"] = "5min",
        /,
        *,
        begin: str | datetime.date | None = None,
        end: str | datetime.date | None = None,
    ):
        """
        Метрики `FUTOI` по заданным параметрам.
        """
        path = await self.get_path("futoi")
        period, begin, end = normalize_tickers_params(period, begin, end)
        if not period in (
            Period.FIVE_MINUTES,
            Period.ONE_DAY,
        ):
            raise ValueError(f"Period {period} not implemented for this dataset")

        params = {"from": begin.isoformat(), "till": end.isoformat(), "interval": period.value}
        return await normalize_futoi(request_futoi(self._client, path, params))
