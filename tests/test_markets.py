from collections.abc import AsyncIterator

from moexsrc import Market
from moexsrc.utils import rollup
import moexsrc.tickers


async def test_markets(client):
    #
    eq = Market("EQ")
    assert str(eq) == 'Market("EQ")'
    #
    fo = Market("FO")
    assert str(fo) == 'Market("FO")'


async def test_markets_tickers(client):
    #
    eq = Market("EQ")
    ait = eq.get_tickers()
    assert isinstance(ait, AsyncIterator)
    tickers = await rollup(ait)
    assert tickers and all(isinstance(ticker, moexsrc.tickers.Ticker) for ticker in tickers)
    assert len([ticker for ticker in tickers if ticker.symbol in ("MOEX", "SBER")]) == 2

    found = await rollup(eq.get_tickers(isin="RU000A0JR4A1"))
    assert len(found) == 1 and found[0].symbol == "MOEX"

    #
    fo = Market("FO")
    ait = fo.get_tickers()
    assert isinstance(ait, AsyncIterator)
    tickers = await rollup(ait)
    assert tickers and all(isinstance(ticker, moexsrc.tickers.Ticker) for ticker in tickers)
    assert len([ticker for ticker in tickers if ticker.symbol in ("IMOEXF", "SBERF")]) == 2

    found = await rollup(fo.get_tickers(assetcode="SILV"))
    assert len(found) > 1 and all(ticker.symbol.startswith("SV") for ticker in found)
