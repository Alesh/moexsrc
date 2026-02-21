import moexsrc.markets
import moexsrc.tickers
import moexsrc.session


class Market(moexsrc.markets.Market):
    def __init__(self, arg: str, *args: str):
        super().__init__(moexsrc.session.ctx, arg, *args)


class Ticker(moexsrc.tickers.Ticker):
    def __init__(self, symbol: str):
        super().__init__(moexsrc.session.ctx, symbol)
