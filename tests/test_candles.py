from moexsrc.iss_client import ISSClient
from moexsrc.tickers import Ticker
from moexsrc.types import Period


async def test_ticker_candles(api_key):
    client = ISSClient(api_key)

    moex = Ticker(client, "MOEX")
    data = await moex.candles("1h", begin="2025-01-01", end="2025-12-31")
    assert data and len(data) > 5000

    sber = Ticker(client, "SBER")
    data = await sber.candles(Period.ONE_WEEK, begin="2025-01-01", end="2025-12-31")
    assert data and len(data) > 50

    moex = Ticker(client, "MOEX")
    data = await moex.candles(begin="2025-12-25", end="2025-12-25")
    assert data and len(data) > 100
