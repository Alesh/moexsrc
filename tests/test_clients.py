from moexsrc.utils import rollup


async def test_iss_request(client, check_fields):
    path = "engines/stock/markets/shares/boards/TQBR/securities/MOEX/candles"
    params = {"from": "2026-02-20", "till": "2026-02-20", "interval": 1}
    candles = await rollup(client.request(path, "candles", **params))
    assert candles and len(candles) == 939
    assert check_fields(candles[0], ("open", "close", "high", "low", "value", "volume", "begin", "end"))


async def test_iss_get_security(client, check_fields):
    #
    security = await client.get_security("MOEX")
    assert check_fields(security, ("boardid", "engine", "is_traded", "market", "secid"))

    security = await client.get_security("SBERF")
    assert check_fields(security, ("boardid", "engine", "is_traded", "market", "secid", "assetcode"))


async def test_iss_get_market_securities(client, check_fields):
    #
    tqbr_securities = await rollup(client.get_market_securities("stock", "shares", "TQBR"))
    assert tqbr_securities and check_fields(tqbr_securities[0], ("boardid", "engine", "is_traded", "market", "secid"))
    all_securities = await rollup(client.get_market_securities("stock", "shares"))
    assert len(all_securities) > len(tqbr_securities)

    rfud_securities = await rollup(client.get_market_securities("futures", "forts", "RFUD"))
    assert check_fields(rfud_securities[0], ("boardid", "engine", "is_traded", "market", "secid"))
    all_securities = await rollup(client.get_market_securities("futures", "forts"))
    assert len(all_securities) == len(rfud_securities)
