from _pytest.main import Session

from moexsrc.iss_client import ISSClient
from moexsrc.markets import Market
from moexsrc.tickers import Ticker
from moexsrc.types import Period


async def test_market_futoi_5min(api_key):
    client = ISSClient(api_key)
    market = Market(client, "FO")
    data = await market.futoi(date="2026-02-02")
    assert data > 72000


async def test_market_futoi_1D(api_key):
    client = ISSClient(api_key)
    market = Market(client, "FO")
    data = await market.futoi("1D", date="2026-02-02")
    assert data


async def test_ticker_futoi_5min(api_key):
    client = ISSClient(api_key)
    ticker = Ticker(client, "CNYRUBF")
    data = await ticker.futoi(begin="2026-02-01", end="2026-02-12")
    assert data and len(data) > 3500

    ticker = Ticker(client, "YDH6")
    data = await ticker.futoi(begin="2026-02-01", end="2026-02-12")
    assert data > 3700


async def test_ticker_futoi_1D(api_key):
    client = ISSClient(api_key)
    ticker = Ticker(client, "AEH6")
    data = await ticker.futoi(Period.ONE_DAY, begin="2026-02-02", end="2026-02-05")
    assert data and len(data) == 3
