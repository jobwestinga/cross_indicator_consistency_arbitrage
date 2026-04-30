from __future__ import annotations

import sqlite3
import subprocess
import sys
from pathlib import Path


SCRIPT_PATH = Path("analysis/inspect_dataset.py")


def test_inspect_dataset_script_runs_on_sample_export(tmp_path: Path) -> None:
    dataset_path = tmp_path / "forecast_analysis_dataset_sample.sqlite"

    with sqlite3.connect(dataset_path) as conn:
        conn.executescript(
            """
            CREATE TABLE export_manifest (
                generated_at TEXT NOT NULL,
                underlying_conid INTEGER,
                since TEXT
            );
            INSERT INTO export_manifest (generated_at, underlying_conid, since)
            VALUES ('2026-04-15T18:37:14+00:00', NULL, NULL);

            CREATE TABLE export_tables (
                table_name TEXT NOT NULL,
                rows INTEGER NOT NULL
            );
            INSERT INTO export_tables (table_name, rows) VALUES
                ('market_categories', 1),
                ('markets', 1),
                ('contracts', 2),
                ('projected_probabilities', 2),
                ('open_interest_snapshots', 2),
                ('contract_history', 2);

            CREATE TABLE market_categories (
                category_key TEXT,
                category_name TEXT
            );
            INSERT INTO market_categories VALUES ('crypto', 'Crypto');

            CREATE TABLE markets (
                underlying_conid INTEGER,
                market_name TEXT,
                symbol TEXT,
                category_key TEXT,
                exchange TEXT,
                payout REAL,
                active INTEGER,
                first_seen_at TEXT,
                last_seen_at TEXT,
                last_discovered_at TEXT,
                last_structure_collected_at TEXT,
                last_probabilities_collected_at TEXT
            );
            INSERT INTO markets VALUES
                (
                    1001,
                    'BTC Price',
                    'CBBTC',
                    'crypto',
                    'FORECASTX',
                    1.0,
                    1,
                    '2026-03-20T00:00:00+00:00',
                    '2026-04-15T00:00:00+00:00',
                    '2026-04-15T00:00:00+00:00',
                    '2026-04-15T00:00:00+00:00',
                    '2026-04-15T18:19:08+00:00'
                );

            CREATE TABLE contracts (
                conid INTEGER,
                underlying_conid INTEGER,
                side TEXT,
                strike_label TEXT,
                expiration TEXT,
                question TEXT,
                active INTEGER
            );
            INSERT INTO contracts VALUES
                (2001, 1001, 'Y', 'Above 100k', '2026-05-01', 'BTC above 100k?', 1),
                (2002, 1001, 'N', 'Below 100k', '2026-05-01', 'BTC below 100k?', 1);

            CREATE TABLE projected_probabilities (
                underlying_conid INTEGER,
                collected_at TEXT
            );
            INSERT INTO projected_probabilities VALUES
                (1001, '2026-04-15T17:00:00+00:00'),
                (1001, '2026-04-15T18:00:00+00:00');

            CREATE TABLE open_interest_snapshots (
                conid INTEGER,
                collected_at TEXT
            );
            INSERT INTO open_interest_snapshots VALUES
                (2001, '2026-04-15T17:30:00+00:00'),
                (2002, '2026-04-15T18:30:00+00:00');

            CREATE TABLE contract_history (
                conid INTEGER,
                period_requested TEXT,
                ts_utc TEXT
            );
            INSERT INTO contract_history VALUES
                (2001, '1week', '2026-04-14T00:00:00+00:00'),
                (2002, '1month', '2026-04-15T00:00:00+00:00');
            """
        )

    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT_PATH),
            str(dataset_path),
            "--underlying-conid",
            "1001",
            "--top-n",
            "5",
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert "Dataset Summary" in result.stdout
    assert "Time Coverage" in result.stdout
    assert "Market Overview" in result.stdout
    assert "Market 1001" in result.stdout
    assert "BTC Price" in result.stdout
    assert "open_interest_snapshots" in result.stdout
