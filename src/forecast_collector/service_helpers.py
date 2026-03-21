from __future__ import annotations


def limit_items[T](items: list[T], limit: int | None) -> list[T]:
    if limit is None:
        return items
    return items[:limit]
