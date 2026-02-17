import typing as t

from moexsrc.iss_client import ISSClient


def check_security_eq(security):
    match security:
        case {
            "boardid": _0,
            "decimals": _1,
            "engine": _2,
            "is_traded": _4,
            "lotsize": _5,
            "market": _6,
            "secid": _7,
        }:
            pass
        case _:
            assert False, "Required fields is not set"


def check_security_fo(security):
    match security:
        case {
            "boardid": _0,
            "decimals": _1,
            "engine": _2,
            "is_traded": _4,
            "lotsize": _5,
            "market": _6,
            "secid": _7,
            "assetcode": _8,
            "sectype": _9,
        }:
            pass
        case _:
            assert False, "Required fields is not set"


async def test_iss_client_security(api_key):
    client = ISSClient(api_key)
    security = await client.security("MOEX")
    check_security_eq(security)


async def test_iss_client_securities(api_key):
    client = ISSClient(api_key)
    securities_ = await securities(client, "stock", "shares", "TQBR")
    for security in securities_:
        check_security_eq(security)

    securities_ = await securities(client, "currency", "selt", "CETS")
    for security in securities_:
        check_security_eq(security)

    securities_ = await securities(client, "futures", "forts")
    for security in securities_:
        check_security_fo(security)
