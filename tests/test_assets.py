from collections.abc import AsyncIterator
from datetime import datetime, date

import pytest
from moexsrc.assets import Asset
from moexsrc.session import Session
from moexsrc.types import Period
from moexsrc.utils import rollup
import moexsrc.tickers


@pytest.fixture
def check_futoi_fields(check_fields):
    def check_candle_fields_(item):
        return check_fields(
            item,
            {
                "assetcode": str,
                "clgroup": str,
                "period": Period,
                "pos": float,
                "pos_long": float,
                "pos_long_num": int,
                "pos_short": float,
                "pos_short_num": int,
                "sectype": str,
                "sess_id": int,
                "session_date": date,
                "seqnum": int,
                "systime": datetime,
                "tradetime": datetime,
            },
        )

    return check_candle_fields_


async def test_assets(token):

    with Session(token) as ctx:
        silv = Asset(ctx, "SILV")
        assert silv.symbol == "SILV"

        ait = silv.get_tickers()
        assert isinstance(ait, AsyncIterator)
        tickers = await rollup(ait)
        assert tickers and all(isinstance(ticker, moexsrc.tickers.Ticker) for ticker in tickers)
        assert len(tickers) >= 2


async def test_assets_futoi(token, check_futoi_fields):

    with Session(token) as ctx:
        silv = Asset(ctx, "SILV")
        futoi = await rollup(silv.futoi(begin="2026-02-02", end="2026-02-06"))
        assert futoi and len(futoi) == 1748
        assert check_futoi_fields(futoi[0])


async def test_assets_dayly_futoi(token, check_futoi_fields):
    with Session(token) as ctx:
        silv = Asset(ctx, "SILV")
        futoi = await rollup(silv.futoi(Period.ONE_DAY, begin="2026-02-02", end="2026-02-06"))
        assert futoi and len(futoi) == 10
        assert check_futoi_fields(futoi[0])


async def test_assets_latest_futoi(token, check_futoi_fields):

    with Session(token) as ctx:
        moex = Asset(ctx, "MOEX")
        futoi = await rollup(moex.futoi(latest=3))
        assert futoi and len(futoi) == 6
        assert check_futoi_fields(futoi[0])


async def test_assets_dayly_latest_futoi(token, check_futoi_fields):

    with Session(token) as ctx:
        moex = Asset(ctx, "MOEX")
        futoi = await rollup(moex.futoi(Period.ONE_DAY, latest=3))
        assert futoi and len(futoi) == 6
        assert check_futoi_fields(futoi[0])
