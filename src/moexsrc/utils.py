import asyncio
import typing as t
from collections.abc import AsyncIterable, AsyncIterator, Iterable, Coroutine, Callable, Iterator
from datetime import datetime, date, time, timedelta


async def rollup(it: AsyncIterable[t.Any]) -> list[t.Any]:
    """ "Сворачивает" асинхронный итератор в список."""
    return [a async for a in it]


def extract(dict_: dict[str, t.Any], *keys: str) -> tuple[t.Any, ...]:
    """Возвращает кортеж значений соответствующий заданным ключам."""
    return tuple(dict_.get(key) for key in keys)


async def limited(ait: AsyncIterable[t.Any], limit: int) -> AsyncIterator[t.Any]:
    """Лимитирует вывод асинхронного итератора"""
    async for item in ait:
        if limit > 0:
            yield item
            limit -= 1
        else:
            break


async def puffup(it: Iterable[t.Any]) -> AsyncIterator[t.Any]:
    """ "Развернуть" синхронный итератор в асинхронный итератор"""
    for item in it:
        yield item


def to_date(value: str | datetime | t.Any) -> date | None:
    """Пытается сконвертировать переданное значение в date, или None если не применимо."""
    if isinstance(value, str):
        return datetime.fromisoformat(value).date()
    elif isinstance(value, datetime):
        return value.date()
    elif isinstance(value, date):
        return value
    return None


def to_datetime(value: str | t.Any, alignment: t.Literal["begin", "end"] = "begin") -> datetime | None:
    """Пытается сконвертировать переданное значение в date | datetime, или оставляет как есть если это не строка."""
    if isinstance(value, str):
        value = datetime.fromisoformat(value) if len(value) > 10 else date.fromisoformat(value)
    if isinstance(value, date) and not isinstance(value, datetime):
        if alignment == "end":
            return datetime.combine(value, time.max)
        else:
            return datetime.combine(value, time.min)
    elif isinstance(value, datetime):
        return value
    return None


def date_pair_gen(begin: date, end: date, step: int = 2) -> Iterator[tuple[date, date]]:
    if step > 0:
        for N in range(0, (end - begin).days + step, step):
            begin_ = begin + timedelta(days=N)
            end_ = begin + timedelta(days=N + 1)
            if begin_ <= end:
                yield begin_, min(end_, end)


class AsyncTasks(Iterable[asyncio.Task]):
    def __init__(self):
        self._tasks: set[asyncio.Task] = set()

    def __bool__(self) -> bool:
        return bool(len(self._tasks))

    def __iter__(self):
        return iter(self._tasks)

    def run(self, coro: Coroutine[t.Any, t.Any, t.Any]) -> None:
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(lambda x: self._tasks.remove(x))


async def async_up_aiter[A, B](
    aiter: AsyncIterator[A], a2biter: Callable[[A], AsyncIterator[B]], *, timeout: float = 0.1
) -> AsyncIterator[B]:
    async_tasks = AsyncTasks()
    biters: list[AsyncIterator[B]] = list()
    pending: set[asyncio.Task[A]] = set()

    def to_pending(biter: AsyncIterator[B], index: int, timeout_: float = 0):
        #
        async def next_b():
            if timeout_ > 0:
                await asyncio.sleep(timeout_)
            b = await anext(biter)
            return b, index

        pending.add(asyncio.create_task(next_b()))

    async def create_biter(a: A):
        biter = a2biter(a)
        index = len(biters)
        biters.append(biter)
        to_pending(biter, index, timeout)

    async def init():
        async for a in aiter:
            async_tasks.run(create_biter(a))

    async_tasks.run(init())
    while async_tasks or pending:
        if pending:
            done, _ = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                pending.remove(task)
                try:
                    result, index = task.result()
                    yield result
                    to_pending(biters[index], index)
                except StopAsyncIteration:
                    pass
        else:
            await asyncio.sleep(timeout)
