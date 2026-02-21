import typing as t
from collections.abc import AsyncIterable, AsyncIterator


async def rollup(it: AsyncIterable[t.Any]) -> list[t.Any]:
    """ "Сворачивает" асинхронный итератор в список."""
    return [a async for a in it]


def extract(dict_: dict[str, t.Any], *keys: str) -> tuple[t.Any, ...]:
    """Возвращает кортеж значений соответствующий заданным ключам."""
    return tuple(dict_.get(key) for key in keys)
