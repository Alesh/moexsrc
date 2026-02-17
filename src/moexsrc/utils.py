import datetime

from moexsrc.types import Period


def normalize_tickers_params(
    period: Period | str, begin: str | datetime.date | None, end: str | datetime.date | None
) -> tuple[Period, datetime.date, datetime.date]:
    """Нормализует параметры методов тикера."""
    begin = begin or datetime.date.today()
    end = end or datetime.date.today()
    begin = begin if isinstance(begin, datetime.date) else datetime.date.fromisoformat(begin)
    end = end if isinstance(end, datetime.date) else datetime.date.fromisoformat(end)
    period = period if isinstance(period, Period) else Period.from_literal(period)
    return period, begin, end


def normalize_market_params(period: Period | str, date: str | datetime.date | None):
    """Нормализует параметры методов рынка."""
    period, date, _ = normalize_tickers_params(period, date, date)
    return period, date
