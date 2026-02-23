from datetime import date, datetime

import pytest
from moexsrc.tickers import Ticker
from moexsrc.session import Session
from moexsrc.types import Period
from moexsrc.utils import rollup


@pytest.fixture
def check_candle_fields(check_fields):
    def check_candle_fields_(item):
        return check_fields(
            item,
            {
                "begin": (date, datetime),
                "close": float,
                "end": (date, datetime),
                "high": float,
                "low": float,
                "open": float,
                "period": Period,
                "secid": str,
                "value": float,
                "volume": int,
            },
        )

    return check_candle_fields_


async def test_tickers(token, check_fields):

    with Session(token) as ctx:
        ticker = Ticker(ctx, "MOEX")
        assert ticker.symbol == "MOEX"


async def test_tickers_candles(token, check_candle_fields):

    with Session(token) as ctx:
        ticker = Ticker(ctx, "MOEX")
        data = await rollup(ticker.candles(Period.ONE_MINUTE, begin="2026-02-20", end="2026-02-20"))
        assert data and len(data) == 939
        assert data[0]["begin"] < data[-1]["begin"]
        assert (data[1]["begin"] - data[0]["begin"]).total_seconds() == 60
        assert check_candle_fields(data[0])

        ticker = Ticker(ctx, "IMOEXF")
        data = await rollup(ticker.candles(Period.ONE_DAY, begin="2026-01-01", end="2026-01-31"))
        assert data and len(data) == 19
        assert check_candle_fields(data[0])


async def test_tickers_resampled_candles(token, check_candle_fields):
    with Session(token) as ctx:
        ticker = Ticker(ctx, "IMOEXF")
        data = await rollup(ticker.candles(Period.FIVE_MINUTES, begin="2026-02-20", end="2026-02-20"))
        assert data and len(data) == 175
        assert (data[1]["begin"] - data[0]["begin"]).total_seconds() == 5 * 60
        assert check_candle_fields(data[0])


async def test_tickers_latest_candles(token, check_candle_fields):
    with Session(token) as ctx:
        ticker = Ticker(ctx, "IMOEXF")
        data = await rollup(ticker.candles(Period.ONE_MINUTE, latest=5))
        assert data and len(data) == 5
        assert data[0]["begin"] > data[-1]["begin"]
        assert check_candle_fields(data[0])


async def test_tickers_resampled_latest_candles(token, check_candle_fields):
    with Session(token) as ctx:
        ticker = Ticker(ctx, "IMOEXF")
        data = await rollup(ticker.candles(Period.FIVE_MINUTES, latest=3))
        assert data and len(data) == 3
        assert (data[0]["begin"] - data[1]["begin"]).total_seconds() == 5 * 60
        assert data[0]["begin"] > data[-1]["begin"]
        assert check_candle_fields(data[0])
