import pytest
from moexsrc.markets import Market
from moexsrc.session import Session
from moexsrc.utils import rollup
import moexsrc.tickers
import moexsrc.assets


async def test_markets_tickers(token):
    #
    with Session(token) as ctx:
        eq = Market(ctx, "EQ")
        tickers = await rollup(eq.get_tickers())
        assert tickers and all(isinstance(ticker, moexsrc.tickers.Ticker) for ticker in tickers)
        assert len([ticker for ticker in tickers if ticker.symbol in ("MOEX", "SBER")]) == 2

        found = await rollup(eq.get_tickers(isin="RU000A0JR4A1"))
        assert len(found) == 1 and found[0].symbol == "MOEX"

        #
        fo = Market(ctx, "FO")
        tickers = await rollup(fo.get_tickers())
        assert tickers and all(isinstance(ticker, moexsrc.tickers.Ticker) for ticker in tickers)
        assert len([ticker for ticker in tickers if ticker.symbol in ("IMOEXF", "SBERF")]) == 2

        found = await rollup(fo.get_tickers(assetcode="SILV"))
        assert len(found) > 1 and all(ticker.symbol.startswith("SV") for ticker in found)


async def test_markets_assets(token):

    with Session(token) as ctx:
        fo = Market(ctx, "FO")
        assets = await rollup(fo.get_assets())
        assert assets and all(isinstance(asset, moexsrc.assets.Asset) for asset in assets)
        assert len([asset for asset in assets if asset.symbol in ("SILV", "IMOEX", "SBERF")]) == 3

        assets = await rollup(fo.get_assets("SILV", "IMOEX", "SBERF"))
        assert len(assets) == 3 and all(isinstance(asset, moexsrc.assets.Asset) for asset in assets)

        eq = Market(ctx, "EQ")
        with pytest.raises(NotImplementedError):
            await rollup(eq.get_assets())
