import asyncio
import json
from datetime import datetime, date, time
import typing as t

import httpx
from moexsrc.types import FutOI


def normalize_futoi(**data: t.Any) -> FutOI:
    """Нормализует данные FutOI."""
    match data:
        case {
            "assetcode": assetcode,
            "clgroup": clgroup,
            "pos": pos,
            "pos_long": pos_long,
            "pos_long_num": pos_long_num,
            "pos_short": pos_short,
            "pos_short_num": pos_short_num,
            "systime": systime,
            "ticker": ticker,
            "tradedate": tradedate,
            "tradetime": tradetime,
            "period": period,
            **other,  # "seqnum", "sess_id", "trade_session_date",
        }:
            return FutOI(
                sectype=ticker,
                clgroup=clgroup.upper(),
                pos=float(pos or 0),
                pos_long=float(pos_long or 0),
                pos_long_num=int(pos_long_num or 0),
                pos_short=float(pos_short or 0),
                pos_short_num=int(pos_short_num or 0),
                tradetime=datetime.combine(date.fromisoformat(tradedate), time.fromisoformat(tradetime)),
                period=period,
                assetcode=assetcode,
                seqnum=other.get("seqnum", 0),
                sess_id=other.get("sess_id", 0),
                session_date=date.fromisoformat(other.get("trade_session_date", tradedate) or tradedate),
                systime=datetime.fromisoformat(systime),
            )
        case _:
            raise ValueError("Wrong FutOI data")


async def daily_futoi(symbol: str, *dates: date):
    """Дневные данные FUTOI с сайта."""

    def to_int(value):
        return int(float(value.replace(",", ".").replace("\xa0", "")))

    def convert_data(data: dict[str, t.Any]) -> list[dict[str, t.Any]]:
        common = dict(
            tradetime="19:00:00",
            tradedate=datetime.strptime(data["clients"]["Date"], "%d.%m.%Y").date().isoformat(),
            systime=datetime.now().isoformat(" ", "seconds"),
        )
        fiz_pos_long = to_int(data["contracts"]["PhysicalLong"])
        fiz_pos_shor = to_int(data["contracts"]["PhysicalShort"])
        yur_pos_long = to_int(data["contracts"]["JuridicalLong"])
        yur_pos_shor = to_int(data["contracts"]["JuridicalShort"])
        return [
            dict(
                clgroup="FIZ",
                pos=fiz_pos_long - fiz_pos_shor,
                pos_long=fiz_pos_long,
                pos_short=-fiz_pos_shor,
                pos_long_num=to_int(data["clients"]["PhysicalLong"]),
                pos_short_num=to_int(data["clients"]["PhysicalShort"]),
                **common,
            ),
            dict(
                clgroup="YUR",
                pos=yur_pos_long - yur_pos_shor,
                pos_long=yur_pos_long,
                pos_short=-yur_pos_shor,
                pos_long_num=to_int(data["clients"]["JuridicalLong"]),
                pos_short_num=to_int(data["clients"]["JuridicalShort"]),
                **common,
            ),
        ]

    e = None
    timeout = 1.0
    line_types = ["contracts", "daily_change", "pct_change", "clients"]
    async with httpx.AsyncClient(timeout=timeout) as client:
        for date_ in dates:
            cnt = 3
            path = f"https://www.moex.com/api/contract/OpenOptionService/{date_.isoformat()}/F/{symbol}/json"
            while cnt:
                cnt -= 1
                try:
                    resp = await client.get(path, timeout=timeout)
                    break
                except httpx.HTTPStatusError:
                    await asyncio.sleep(timeout)
                    timeout += timeout
            else:
                raise e
            if resp.is_success:
                if resp.headers.get("content-type", "").startswith("application/json"):
                    if data := json.loads(resp.text):
                        for item in convert_data(dict((line_types[N], data[N]) for N in range(len(data)))):
                            yield item
                        continue
                resp.status_code = 400
            resp.raise_for_status()
