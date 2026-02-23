from datetime import date

from moexsrc.utils import date_pair_gen


def test_date_pair_gen():
    dates = list(date_pair_gen(date(2026, 1, 1), date(2026, 1, 5), 2))
    assert dates == [
        (date(2026, 1, 1), date(2026, 1, 2)),
        (date(2026, 1, 3), date(2026, 1, 4)),
        (date(2026, 1, 5), date(2026, 1, 5)),
    ]

    dates = list(date_pair_gen(date(2026, 1, 1), date(2026, 1, 5), 1))
    assert [d for d, _ in dates] == [
        date(2026, 1, 1),
        date(2026, 1, 2),
        date(2026, 1, 3),
        date(2026, 1, 4),
        date(2026, 1, 5),
    ]
