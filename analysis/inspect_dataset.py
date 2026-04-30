from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path
from typing import Iterable, Sequence


DEFAULT_PATTERN = "forecast_analysis_dataset*.sqlite"


def format_int(value: object) -> str:
    if value is None:
        return "-"
    return f"{int(value):,}"


def format_text(value: object) -> str:
    if value is None:
        return "-"
    text = str(value)
    return text if text else "-"


def format_bytes(size: int) -> str:
    value = float(size)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if value < 1024 or unit == "TB":
            if unit == "B":
                return f"{int(value)} {unit}"
            return f"{value:.1f} {unit}"
        value /= 1024
    return f"{size} B"


def render_table(headers: Sequence[str], rows: Sequence[Sequence[object]]) -> str:
    widths = [len(header) for header in headers]
    normalized_rows = [[format_text(value) for value in row] for row in rows]
    for row in normalized_rows:
        for index, value in enumerate(row):
            widths[index] = max(widths[index], len(value))

    lines = [
        "  ".join(header.ljust(widths[index]) for index, header in enumerate(headers)),
        "  ".join("-" * widths[index] for index in range(len(headers))),
    ]
    for row in normalized_rows:
        lines.append("  ".join(value.ljust(widths[index]) for index, value in enumerate(row)))
    return "\n".join(lines)


def print_section(title: str, lines: Iterable[str]) -> None:
    print(title)
    for line in lines:
        print(line)
    print()


def resolve_dataset_path(raw_path: str | None) -> Path:
    if raw_path:
        path = Path(raw_path).expanduser().resolve()
        if not path.exists():
            raise FileNotFoundError(f"Dataset file not found: {path}")
        return path

    candidates = [
        path
        for search_root in (Path.cwd(), Path.home())
        for path in search_root.glob(DEFAULT_PATTERN)
        if path.is_file()
    ]
    if not candidates:
        raise FileNotFoundError(
            "No dataset path provided and no local file matching "
            f"'{DEFAULT_PATTERN}' was found in the current directory or home directory."
        )
    return max(candidates, key=lambda path: path.stat().st_mtime).resolve()


def query_rows(conn: sqlite3.Connection, query: str, params: Sequence[object] = ()) -> list[sqlite3.Row]:
    return conn.execute(query, params).fetchall()


def query_one(conn: sqlite3.Connection, query: str, params: Sequence[object] = ()) -> sqlite3.Row:
    row = conn.execute(query, params).fetchone()
    if row is None:
        raise RuntimeError(f"Expected a row for query: {query}")
    return row


def get_table_names(conn: sqlite3.Connection) -> set[str]:
    return {
        row["name"]
        for row in query_rows(
            conn,
            "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name",
        )
    }


def build_summary_lines(conn: sqlite3.Connection, dataset_path: Path) -> list[str]:
    lines = [
        f"Dataset: {dataset_path}",
        f"File Size: {format_bytes(dataset_path.stat().st_size)}",
    ]
    table_names = get_table_names(conn)
    if "export_manifest" in table_names:
        manifest = query_one(
            conn,
            """
            SELECT generated_at, underlying_conid, since
            FROM export_manifest
            LIMIT 1
            """,
        )
        lines.extend(
            [
                f"Generated At: {format_text(manifest['generated_at'])}",
                f"Underlying Filter: {format_text(manifest['underlying_conid'])}",
                f"Since Filter: {format_text(manifest['since'])}",
            ]
        )

    if "export_tables" in table_names:
        table_rows = query_rows(
            conn,
            "SELECT table_name, rows FROM export_tables ORDER BY rows DESC, table_name",
        )
        total_rows = sum(int(row["rows"]) for row in table_rows)
        lines.append(f"Exported Rows: {format_int(total_rows)}")
        lines.append("Tables:")
        lines.append(
            render_table(
                ("table_name", "rows"),
                [(row["table_name"], format_int(row["rows"])) for row in table_rows],
            )
        )

    return lines


def build_time_coverage_lines(conn: sqlite3.Connection) -> list[str]:
    table_names = get_table_names(conn)
    export_table_lookup = {}
    if "export_tables" in table_names:
        export_table_lookup = {
            row["table_name"]: row["rows"]
            for row in query_rows(conn, "SELECT table_name, rows FROM export_tables")
        }

    rows: list[tuple[object, ...]] = []

    if "projected_probabilities" in table_names:
        summary = query_one(
            conn,
            """
            SELECT MIN(collected_at) AS min_ts, MAX(collected_at) AS max_ts
            FROM projected_probabilities
            """,
        )
        rows.append(
            (
                "projected_probabilities",
                format_int(export_table_lookup.get("projected_probabilities")),
                format_text(summary["min_ts"]),
                format_text(summary["max_ts"]),
            )
        )

    if "open_interest_snapshots" in table_names:
        summary = query_one(
            conn,
            """
            SELECT MIN(collected_at) AS min_ts, MAX(collected_at) AS max_ts
            FROM open_interest_snapshots
            """,
        )
        rows.append(
            (
                "open_interest_snapshots",
                format_int(export_table_lookup.get("open_interest_snapshots")),
                format_text(summary["min_ts"]),
                format_text(summary["max_ts"]),
            )
        )

    if "contract_history" in table_names:
        summary = query_one(
            conn,
            """
            SELECT MIN(ts_utc) AS min_ts, MAX(ts_utc) AS max_ts
            FROM contract_history
            """,
        )
        rows.append(
            (
                "contract_history",
                format_int(export_table_lookup.get("contract_history")),
                format_text(summary["min_ts"]),
                format_text(summary["max_ts"]),
            )
        )

    lines = [
        render_table(
            ("table", "rows", "min_ts", "max_ts"),
            rows,
        )
    ]

    if "contract_history" in table_names:
        period_rows = query_rows(
            conn,
            """
            SELECT period_requested, COUNT(*) AS row_count
            FROM contract_history
            GROUP BY period_requested
            ORDER BY row_count DESC, period_requested
            """,
        )
        if period_rows:
            lines.append("History Periods:")
            lines.append(
                render_table(
                    ("period_requested", "rows"),
                    [(row["period_requested"], format_int(row["row_count"])) for row in period_rows],
                )
            )

    return lines


def build_market_overview_lines(conn: sqlite3.Connection, top_n: int) -> list[str]:
    table_names = get_table_names(conn)
    lines: list[str] = []

    if {"markets", "contracts"}.issubset(table_names):
        counts = query_one(
            conn,
            """
            SELECT
                (SELECT COUNT(*) FROM markets) AS total_markets,
                (SELECT COUNT(*) FROM markets WHERE COALESCE(active, 1) = 1) AS active_markets,
                (SELECT COUNT(*) FROM contracts) AS total_contracts,
                (SELECT COUNT(*) FROM contracts WHERE COALESCE(active, 1) = 1) AS active_contracts
            """,
        )
        lines.extend(
            [
                f"Markets: {format_int(counts['total_markets'])} total / {format_int(counts['active_markets'])} active",
                f"Contracts: {format_int(counts['total_contracts'])} total / {format_int(counts['active_contracts'])} active",
            ]
        )

        category_rows = query_rows(
            conn,
            """
            SELECT
                COALESCE(category_key, '(uncategorized)') AS category_key,
                COUNT(*) AS market_count
            FROM markets
            GROUP BY COALESCE(category_key, '(uncategorized)')
            ORDER BY market_count DESC, category_key
            LIMIT ?
            """,
            (top_n,),
        )
        if category_rows:
            lines.append("Top Categories:")
            lines.append(
                render_table(
                    ("category_key", "market_count"),
                    [(row["category_key"], format_int(row["market_count"])) for row in category_rows],
                )
            )

        contract_rows = query_rows(
            conn,
            """
            SELECT
                m.underlying_conid,
                m.symbol,
                m.market_name,
                COUNT(*) AS contract_count
            FROM contracts AS c
            JOIN markets AS m
              ON m.underlying_conid = c.underlying_conid
            GROUP BY m.underlying_conid, m.symbol, m.market_name
            ORDER BY contract_count DESC, m.underlying_conid
            LIMIT ?
            """,
            (top_n,),
        )
        if contract_rows:
            lines.append(f"Top Markets By Contract Count (top {top_n}):")
            lines.append(
                render_table(
                    ("underlying_conid", "symbol", "market_name", "contract_count"),
                    [
                        (
                            row["underlying_conid"],
                            row["symbol"],
                            row["market_name"],
                            format_int(row["contract_count"]),
                        )
                        for row in contract_rows
                    ],
                )
            )

    return lines


def build_focus_lines(
    conn: sqlite3.Connection,
    underlying_conid: int,
    contract_sample_limit: int,
    include_heavy: bool,
) -> list[str]:
    table_names = get_table_names(conn)
    lines: list[str] = []

    market = conn.execute(
        """
        SELECT
            underlying_conid,
            market_name,
            symbol,
            category_key,
            exchange,
            payout,
            active,
            first_seen_at,
            last_seen_at,
            last_discovered_at,
            last_structure_collected_at,
            last_probabilities_collected_at
        FROM markets
        WHERE underlying_conid = ?
        """,
        (underlying_conid,),
    ).fetchone()
    if market is None:
        return [f"Market {underlying_conid} was not found in this dataset."]

    lines.extend(
        [
            f"Market: {format_text(market['market_name'])}",
            f"Symbol: {format_text(market['symbol'])}",
            f"Category: {format_text(market['category_key'])}",
            f"Exchange: {format_text(market['exchange'])}",
            f"Active: {format_text(market['active'])}",
            f"Payout: {format_text(market['payout'])}",
            f"First Seen: {format_text(market['first_seen_at'])}",
            f"Last Seen: {format_text(market['last_seen_at'])}",
            f"Last Discovery: {format_text(market['last_discovered_at'])}",
            f"Last Structure Collection: {format_text(market['last_structure_collected_at'])}",
            f"Last Probability Collection: {format_text(market['last_probabilities_collected_at'])}",
        ]
    )

    contract_counts = query_one(
        conn,
        """
        SELECT
            COUNT(*) AS total_contracts,
            SUM(CASE WHEN COALESCE(active, 1) = 1 THEN 1 ELSE 0 END) AS active_contracts
        FROM contracts
        WHERE underlying_conid = ?
        """,
        (underlying_conid,),
    )
    lines.append(
        f"Contracts: {format_int(contract_counts['total_contracts'])} total / "
        f"{format_int(contract_counts['active_contracts'])} active"
    )

    contract_samples = query_rows(
        conn,
        """
        SELECT
            conid,
            side,
            strike_label,
            expiration,
            question
        FROM contracts
        WHERE underlying_conid = ?
        ORDER BY conid
        LIMIT ?
        """,
        (underlying_conid, contract_sample_limit),
    )
    if contract_samples:
        lines.append(f"Sample Contracts (first {contract_sample_limit}):")
        lines.append(
            render_table(
                ("conid", "side", "strike_label", "expiration", "question"),
                [
                    (
                        row["conid"],
                        row["side"],
                        row["strike_label"],
                        row["expiration"],
                        row["question"],
                    )
                    for row in contract_samples
                ],
            )
        )

    if include_heavy:
        if "projected_probabilities" in table_names:
            summary = query_one(
                conn,
                """
                SELECT COUNT(*) AS row_count, MIN(collected_at) AS min_ts, MAX(collected_at) AS max_ts
                FROM projected_probabilities
                WHERE underlying_conid = ?
                """,
                (underlying_conid,),
            )
            lines.append(
                "Projected Probabilities: "
                f"{format_int(summary['row_count'])} rows, "
                f"{format_text(summary['min_ts'])} -> {format_text(summary['max_ts'])}"
            )

        if {"open_interest_snapshots", "contracts"}.issubset(table_names):
            summary = query_one(
                conn,
                """
                SELECT COUNT(*) AS row_count, MIN(s.collected_at) AS min_ts, MAX(s.collected_at) AS max_ts
                FROM open_interest_snapshots AS s
                JOIN contracts AS c
                  ON c.conid = s.conid
                WHERE c.underlying_conid = ?
                """,
                (underlying_conid,),
            )
            lines.append(
                "Open Interest: "
                f"{format_int(summary['row_count'])} rows, "
                f"{format_text(summary['min_ts'])} -> {format_text(summary['max_ts'])}"
            )

        if {"contract_history", "contracts"}.issubset(table_names):
            summary = query_one(
                conn,
                """
                SELECT COUNT(*) AS row_count, MIN(h.ts_utc) AS min_ts, MAX(h.ts_utc) AS max_ts
                FROM contract_history AS h
                JOIN contracts AS c
                  ON c.conid = h.conid
                WHERE c.underlying_conid = ?
                """,
                (underlying_conid,),
            )
            lines.append(
                "History: "
                f"{format_int(summary['row_count'])} rows, "
                f"{format_text(summary['min_ts'])} -> {format_text(summary['max_ts'])}"
            )
    else:
        lines.append(
            "Heavy time-series summaries skipped. Re-run with --include-heavy if you want "
            "per-market counts for probabilities, open interest, and history."
        )

    return lines


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Inspect a local ForecastTrader SQLite export and print a fast, "
            "human-readable summary."
        )
    )
    parser.add_argument(
        "dataset",
        nargs="?",
        help=(
            "Path to a SQLite export. If omitted, the script picks the newest "
            f"local file matching '{DEFAULT_PATTERN}'."
        ),
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=10,
        help="How many categories and markets to show in ranking sections.",
    )
    parser.add_argument(
        "--underlying-conid",
        action="append",
        type=int,
        default=[],
        help="Optional market to inspect in more detail. Repeat for multiple markets.",
    )
    parser.add_argument(
        "--contract-sample-limit",
        type=int,
        default=8,
        help="How many sample contracts to show for each focused market.",
    )
    parser.add_argument(
        "--include-heavy",
        action="store_true",
        help=(
            "Include extra per-market time-series summaries. This can be slower "
            "on very large exports."
        ),
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    dataset_path = resolve_dataset_path(args.dataset)

    conn = sqlite3.connect(dataset_path)
    conn.row_factory = sqlite3.Row

    try:
        print_section("Dataset Summary", build_summary_lines(conn, dataset_path))
        print_section("Time Coverage", build_time_coverage_lines(conn))
        print_section("Market Overview", build_market_overview_lines(conn, args.top_n))

        for underlying_conid in args.underlying_conid:
            print_section(
                f"Market {underlying_conid}",
                build_focus_lines(
                    conn,
                    underlying_conid=underlying_conid,
                    contract_sample_limit=args.contract_sample_limit,
                    include_heavy=args.include_heavy,
                ),
            )
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
