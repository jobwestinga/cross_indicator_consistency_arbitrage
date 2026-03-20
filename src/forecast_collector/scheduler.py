from __future__ import annotations

import json

from .models import ScheduleDefinition


def build_schedule() -> list[ScheduleDefinition]:
    return [
        ScheduleDefinition(
            name="market-structure",
            interval_seconds=6 * 60 * 60,
            command=["collect-seed-market"],
        ),
        ScheduleDefinition(
            name="history",
            interval_seconds=60 * 60,
            command=["collect-history"],
        ),
        ScheduleDefinition(
            name="open-interest",
            interval_seconds=15 * 60,
            command=["collect-open-interest"],
        ),
        ScheduleDefinition(
            name="projected-probabilities",
            interval_seconds=6 * 60 * 60,
            command=["collect-probabilities"],
        ),
    ]


def main() -> None:
    print(json.dumps([item.model_dump() for item in build_schedule()], indent=2))


if __name__ == "__main__":
    main()
