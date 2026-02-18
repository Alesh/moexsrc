from moexsrc.iss_client import ISSClient


def check_security_eq(security):
    requires = ("boardid", "decimals", "engine", "is_traded", "lotsize", "market", "secid")
    founds = [field in security for field in requires]
    assert all(founds), (
        f"Required fields: {[requires[N] for N in range(len(requires)) if not founds[N]]} has not been set"
    )


def check_security_fo(security):
    requires = ("boardid", "decimals", "engine", "is_traded", "lotsize", "market", "secid", "assetcode", "sectype")
    founds = [field in security for field in requires]
    assert all(founds), (
        f"Required fields: {[requires[N] for N in range(len(requires)) if not founds[N]]} has not been set"
    )


async def test_iss_client_security(api_key):
    client = ISSClient(api_key)
    security = await client.security("MOEX")
    check_security_eq(security)


async def test_iss_client_securities(client):

    async for security in client.securities("stock", "shares", "TQBR", fullinfo=True):
        check_security_eq(security)

    async for security in client.securities("currency", "selt", "CETS", fullinfo=True):
        check_security_eq(security)

    async for security in client.securities("futures", "forts", fullinfo=True):
        check_security_fo(security)
