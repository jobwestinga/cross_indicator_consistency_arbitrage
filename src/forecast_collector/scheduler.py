from __future__ import annotations

import argparse
import json
from pathlib import Path

from .models import ScheduleDefinition


def build_schedule() -> list[ScheduleDefinition]:
    return [
        ScheduleDefinition(
            name="forecast-discover",
            interval_seconds=60 * 60,
            command=["docker", "compose", "run", "--rm", "collector", "discover-markets"],
            description="Refresh the public ForecastTrader category tree and market inventory.",
        ),
        ScheduleDefinition(
            name="forecast-structure",
            interval_seconds=6 * 60 * 60,
            command=[
                "docker",
                "compose",
                "run",
                "--rm",
                "collector",
                "collect-market-structures",
                "--all-discovered",
            ],
            description="Refresh ladder and contract-detail structure for all active markets.",
        ),
        ScheduleDefinition(
            name="forecast-open-interest",
            interval_seconds=15 * 60,
            command=[
                "docker",
                "compose",
                "run",
                "--rm",
                "collector",
                "collect-open-interest",
                "--all-discovered",
            ],
            description="Collect batched open-interest snapshots for all active markets.",
        ),
        ScheduleDefinition(
            name="forecast-probabilities",
            interval_seconds=6 * 60 * 60,
            command=[
                "docker",
                "compose",
                "run",
                "--rm",
                "collector",
                "collect-probabilities",
                "--all-discovered",
            ],
            description="Collect projected probability ladders for all active markets.",
        ),
        ScheduleDefinition(
            name="forecast-history-incremental",
            interval_seconds=60 * 60,
            command=[
                "docker",
                "compose",
                "run",
                "--rm",
                "collector",
                "collect-history",
                "--all-discovered",
                "--mode",
                "incremental",
            ],
            description="Refresh recent history windows for all active contracts.",
        ),
        ScheduleDefinition(
            name="forecast-history-backfill",
            interval_seconds=24 * 60 * 60,
            command=[
                "docker",
                "compose",
                "run",
                "--rm",
                "collector",
                "collect-history",
                "--all-discovered",
                "--mode",
                "backfill",
            ],
            description="Backfill currently available history windows for all active contracts.",
        ),
    ]


def render_service(definition: ScheduleDefinition, workdir: str) -> str:
    return "\n".join(
        [
            "[Unit]",
            f"Description={definition.description or definition.name}",
            "After=docker.service network-online.target",
            "",
            "[Service]",
            "Type=oneshot",
            f"WorkingDirectory={workdir}",
            f"ExecStart={' '.join(definition.command)}",
            "",
            "[Install]",
            "WantedBy=multi-user.target",
            "",
        ]
    )


def render_timer(definition: ScheduleDefinition) -> str:
    return "\n".join(
        [
            "[Unit]",
            f"Description=Timer for {definition.description or definition.name}",
            "",
            "[Timer]",
            f"OnUnitActiveSec={definition.interval_seconds}",
            "Persistent=true",
            "",
            "[Install]",
            "WantedBy=timers.target",
            "",
        ]
    )


def write_systemd_units(output_dir: Path, workdir: str) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    for definition in build_schedule():
        (output_dir / f"{definition.name}.service").write_text(
            render_service(definition, workdir),
            encoding="utf-8",
        )
        (output_dir / f"{definition.name}.timer").write_text(
            render_timer(definition),
            encoding="utf-8",
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Render systemd timers for the collector.")
    parser.add_argument(
        "--workdir",
        default="/srv/cross_indicator_consistency_arbitrage",
        help="Target working directory on the VPS.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Optional output directory for generated .service/.timer files.",
    )
    args = parser.parse_args()

    if args.output_dir is not None:
        write_systemd_units(args.output_dir, args.workdir)
        return

    print(
        json.dumps(
            [item.model_dump() for item in build_schedule()],
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
