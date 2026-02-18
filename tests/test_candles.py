from moexsrc.iss_client import ISSClient
from moexsrc.tickers import Ticker
from moexsrc.types import Period


async def test_ticker_candles(client):

    moex = Ticker(client, "MOEX")
    data = await moex.candles("1h", begin="2025-01-01", end="2025-12-31")
    assert data and len(data) > 5000

    sber = Ticker(client, "SBER")
    data = await sber.candles(Period.ONE_WEEK, begin="2025-01-01", end="2025-12-31")
    assert data and len(data) > 50

    moex = Ticker(client, "MOEX")
    data = await moex.candles(begin="2025-12-25", end="2025-12-25")
    assert data and len(data) > 100


async def test_ticker_candles_5min(client):

    ticker = Ticker(client, "CNYRUBF")
    data_1min = await ticker.candles("1min", begin="2026-02-02", end="2026-02-06")
    assert data_1min and len(data_1min) > 4000

    data_5min = await ticker.candles("5min", begin="2026-02-02", end="2026-02-06")
    assert data_5min and len(data_1min) / 5 < len(data_5min)
