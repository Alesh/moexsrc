from moexsrc.session import SessionCtx


class Ticker:
    """
    Класс реализует методы для получения информации по рыночному инструменту.
    """

    def __init__(self, ctx: SessionCtx, secid: str):
        self._desc = dict(secid=secid.upper())
        self._ctx = ctx

    def __repr__(self) -> str:
        return f'Ticker("{self.symbol}")'

    def __str__(self) -> str:
        return repr(self)

    @property
    def symbol(self) -> str:
        """Символьное представление инструмента."""
        return self._desc["secid"]
