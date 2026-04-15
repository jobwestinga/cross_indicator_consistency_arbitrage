from __future__ import annotations

import hashlib
import json
import csv
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from io import TextIOBase
from pathlib import Path
from typing import Any, Iterator, Sequence

import psycopg
from psycopg.rows import dict_row

from .models import (
    ApiResponseEnvelope,
    CategoryRecord,
    ContractRecord,
    HealthReport,
    HistoryPoint,
    MarketRecord,
    OpenInterestSnapshot,
    ProjectedProbability,
)


class CollectorRepository:
    def __init__(self, database_url: str) -> None:
        self.database_url = database_url
        self._conn: psycopg.Connection[Any] | None = None

    def __enter__(self) -> "CollectorRepository":
        self._conn = psycopg.connect(
            self.database_url,
            row_factory=dict_row,
            autocommit=True,
        )
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    @property
    def conn(self) -> psycopg.Connection[Any]:
        if self._conn is None:
            raise RuntimeError("Repository connection is not open")
        return self._conn

    @contextmanager
    def transaction(self) -> Iterator[None]:
        with self.conn.transaction():
            yield

    def write_query_csv(
        self,
        cursor_label: str,
        query: str,
        output: TextIOBase,
        params: Sequence[Any] | None = None,
        *,
        fetch_size: int = 10_000,
    ) -> int:
        row_count = 0
        with self.conn.transaction():
            with self.conn.cursor(
                name=f"export_{cursor_label}_{uuid.uuid4().hex[:8]}",
                row_factory=dict_row,
            ) as cur:
                cur.execute(query, tuple(params or ()))
                fieldnames = [column.name for column in (cur.description or ())]
                writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction="ignore")
                writer.writeheader()
                while True:
                    rows = cur.fetchmany(fetch_size)
                    if not rows:
                        break
                    for row in rows:
                        writer.writerow(dict(row))
                        row_count += 1
        output.flush()
        return row_count

    def write_query_sqlite(
        self,
        cursor_label: str,
        query: str,
        sqlite_conn: sqlite3.Connection,
        table_name: str,
        params: Sequence[Any] | None = None,
        *,
        fetch_size: int = 10_000,
    ) -> int:
        row_count = 0
        with self.conn.transaction():
            with self.conn.cursor(
                name=f"export_{cursor_label}_{uuid.uuid4().hex[:8]}",
                row_factory=dict_row,
            ) as cur:
                cur.execute(query, tuple(params or ()))
                fieldnames = [column.name for column in (cur.description or ())]
                if not fieldnames:
                    raise RuntimeError(f"SQLite export query for {cursor_label} returned no columns")

                quoted_table_name = self._quote_sqlite_identifier(table_name)
                quoted_fieldnames = [self._quote_sqlite_identifier(name) for name in fieldnames]
                sqlite_conn.execute(f"DROP TABLE IF EXISTS {quoted_table_name}")
                sqlite_conn.execute(
                    f"CREATE TABLE {quoted_table_name} ({', '.join(quoted_fieldnames)})"
                )

                placeholders = ", ".join("?" for _ in fieldnames)
                sqlite_conn.executemany(
                    f"INSERT INTO {quoted_table_name} ({', '.join(quoted_fieldnames)}) VALUES ({placeholders})",
                    self._iter_sqlite_rows(cur, fieldnames, fetch_size=fetch_size),
                )
                row_count = sqlite_conn.execute(
                    f"SELECT COUNT(*) FROM {quoted_table_name}"
                ).fetchone()[0]

        return int(row_count)

    @staticmethod
    def _iter_sqlite_rows(
        cur: psycopg.Cursor[Any],
        fieldnames: Sequence[str],
        *,
        fetch_size: int,
    ) -> Iterator[tuple[Any, ...]]:
        while True:
            rows = cur.fetchmany(fetch_size)
            if not rows:
                break
            for row in rows:
                yield tuple(
                    CollectorRepository._normalize_sqlite_value(dict(row).get(fieldname))
                    for fieldname in fieldnames
                )

    @staticmethod
    def _normalize_sqlite_value(value: Any) -> Any:
        if isinstance(value, datetime):
            return value.isoformat()
        return value

    @staticmethod
    def _quote_sqlite_identifier(value: str) -> str:
        return '"' + value.replace('"', '""') + '"'

    def run_migrations(self, sql_directory: Path) -> None:
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version TEXT PRIMARY KEY,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        self.conn.commit()

        for path in sorted(sql_directory.glob("*.sql")):
            existing = self.conn.execute(
                "SELECT 1 FROM schema_migrations WHERE version = %s",
                (path.name,),
            ).fetchone()
            if existing:
                continue
            sql = path.read_text(encoding="utf-8")
            with self.conn.transaction():
                self.conn.execute(sql)
                self.conn.execute(
                    "INSERT INTO schema_migrations (version) VALUES (%s)",
                    (path.name,),
                )
            self.conn.commit()

    def acquire_advisory_lock(self, lock_name: str) -> bool:
        row = self.conn.execute(
            "SELECT pg_try_advisory_lock(hashtext(%s)) AS locked",
            (lock_name,),
        ).fetchone()
        return bool(row["locked"])

    def release_advisory_lock(self, lock_name: str) -> None:
        self.conn.execute("SELECT pg_advisory_unlock(hashtext(%s))", (lock_name,))
        self.conn.commit()

    def start_run(self, job_name: str, job_args: dict[str, Any] | None = None) -> int:
        row = self.conn.execute(
            """
            INSERT INTO collection_runs (job_name, status, job_args)
            VALUES (%s, 'running', %s::jsonb)
            RETURNING id
            """,
            (job_name, json.dumps(job_args or {})),
        ).fetchone()
        self.conn.commit()
        return int(row["id"])

    def finish_run(
        self,
        run_id: int,
        status: str,
        error_text: str | None = None,
        summary: dict[str, Any] | None = None,
    ) -> None:
        self.conn.execute(
            """
            UPDATE collection_runs
            SET status = %s,
                error_text = %s,
                summary_json = %s::jsonb,
                finished_at = NOW()
            WHERE id = %s
            """,
            (status, error_text, json.dumps(summary or {}), run_id),
        )
        self.conn.commit()

    def record_raw_response(self, run_id: int | None, response: ApiResponseEnvelope) -> None:
        canonical = json.dumps(response.response_json, sort_keys=True, separators=(",", ":"))
        digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        self.conn.execute(
            """
            INSERT INTO raw_api_responses (
                run_id,
                endpoint_name,
                request_url,
                query_params,
                http_status,
                response_json,
                response_sha256,
                fetched_at
            )
            VALUES (%s, %s, %s, %s::jsonb, %s, %s::jsonb, %s, %s)
            """,
            (
                run_id,
                response.endpoint_name,
                response.request_url,
                json.dumps(response.query_params),
                response.http_status,
                canonical,
                digest,
                response.fetched_at,
            ),
        )

    def upsert_categories(self, categories: Sequence[CategoryRecord]) -> int:
        if not categories:
            return 0
        params = [
            (
                category.category_key,
                category.category_name,
                category.parent_category_key,
                category.first_seen_at,
                category.last_seen_at,
            )
            for category in categories
        ]
        with self.conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO market_categories (
                    category_key,
                    category_name,
                    parent_category_key,
                    first_seen_at,
                    last_seen_at
                )
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (category_key) DO UPDATE SET
                    category_name = EXCLUDED.category_name,
                    parent_category_key = EXCLUDED.parent_category_key,
                    last_seen_at = EXCLUDED.last_seen_at
                """,
                params,
            )
        return len(params)

    def upsert_market(self, market: MarketRecord) -> None:
        self.upsert_markets([market])

    def upsert_markets(self, markets: Sequence[MarketRecord]) -> int:
        if not markets:
            return 0
        params = [
            (
                market.underlying_conid,
                market.market_name,
                market.symbol,
                market.exchange,
                market.product_conid,
                market.category_key,
                market.logo_category,
                market.payout,
                market.exclude_historical_data,
                market.active,
                market.first_seen_at,
                market.last_seen_at,
                market.last_discovered_at,
                market.last_structure_collected_at,
                market.last_probabilities_collected_at,
            )
            for market in markets
        ]
        with self.conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO markets (
                    underlying_conid,
                    market_name,
                    symbol,
                    exchange,
                    product_conid,
                    category_key,
                    logo_category,
                    payout,
                    exclude_historical_data,
                    active,
                    first_seen_at,
                    last_seen_at,
                    last_discovered_at,
                    last_structure_collected_at,
                    last_probabilities_collected_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (underlying_conid) DO UPDATE SET
                    market_name = EXCLUDED.market_name,
                    symbol = EXCLUDED.symbol,
                    exchange = EXCLUDED.exchange,
                    product_conid = COALESCE(EXCLUDED.product_conid, markets.product_conid),
                    category_key = COALESCE(EXCLUDED.category_key, markets.category_key),
                    logo_category = COALESCE(EXCLUDED.logo_category, markets.logo_category),
                    payout = COALESCE(EXCLUDED.payout, markets.payout),
                    exclude_historical_data = COALESCE(EXCLUDED.exclude_historical_data, markets.exclude_historical_data),
                    active = EXCLUDED.active,
                    last_seen_at = EXCLUDED.last_seen_at,
                    last_discovered_at = COALESCE(EXCLUDED.last_discovered_at, markets.last_discovered_at),
                    last_structure_collected_at = COALESCE(EXCLUDED.last_structure_collected_at, markets.last_structure_collected_at),
                    last_probabilities_collected_at = COALESCE(EXCLUDED.last_probabilities_collected_at, markets.last_probabilities_collected_at)
                """,
                params,
            )
        return len(params)

    def mark_missing_markets_inactive(
        self,
        active_underlying_conids: Sequence[int],
        discovered_at: datetime,
    ) -> None:
        if active_underlying_conids:
            self.conn.execute(
                """
                UPDATE markets
                SET active = FALSE
                WHERE underlying_conid <> ALL(%s::bigint[])
                  AND COALESCE(active, TRUE) = TRUE
                """,
                (list(active_underlying_conids),),
            )
        else:
            self.conn.execute("UPDATE markets SET active = FALSE WHERE COALESCE(active, TRUE) = TRUE")
        self.conn.execute(
            """
            UPDATE markets
            SET last_discovered_at = COALESCE(last_discovered_at, %s)
            WHERE last_discovered_at IS NULL
            """,
            (discovered_at,),
        )

    def list_active_markets(self) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT underlying_conid, market_name, symbol, exchange
            FROM markets
            WHERE COALESCE(active, TRUE) = TRUE
            ORDER BY underlying_conid
            """
        ).fetchall()
        return list(rows)

    def upsert_contract(self, contract: ContractRecord) -> None:
        self.upsert_contracts([contract])

    def upsert_contracts(self, contracts: Sequence[ContractRecord]) -> int:
        if not contracts:
            return 0
        params = [
            (
                contract.conid,
                contract.underlying_conid,
                contract.side,
                contract.strike,
                contract.strike_label,
                contract.expiration,
                contract.expiry_label,
                contract.time_specifier,
                contract.question,
                contract.conid_yes,
                contract.conid_no,
                contract.product_conid,
                contract.market_name,
                contract.symbol,
                contract.measured_period,
                contract.measured_period_units,
                contract.active,
                contract.first_seen_at,
                contract.last_seen_at,
                contract.last_details_collected_at,
                contract.last_open_interest_collected_at,
                contract.last_history_collected_at,
                contract.last_history_no_data_at,
            )
            for contract in contracts
        ]
        with self.conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO contracts (
                    conid,
                    underlying_conid,
                    side,
                    strike,
                    strike_label,
                    expiration,
                    expiry_label,
                    time_specifier,
                    question,
                    conid_yes,
                    conid_no,
                    product_conid,
                    market_name,
                    symbol,
                    measured_period,
                    measured_period_units,
                    active,
                    first_seen_at,
                    last_seen_at,
                    last_details_collected_at,
                    last_open_interest_collected_at,
                    last_history_collected_at,
                    last_history_no_data_at
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (conid) DO UPDATE SET
                    underlying_conid = EXCLUDED.underlying_conid,
                    side = COALESCE(EXCLUDED.side, contracts.side),
                    strike = COALESCE(EXCLUDED.strike, contracts.strike),
                    strike_label = COALESCE(EXCLUDED.strike_label, contracts.strike_label),
                    expiration = COALESCE(EXCLUDED.expiration, contracts.expiration),
                    expiry_label = COALESCE(EXCLUDED.expiry_label, contracts.expiry_label),
                    time_specifier = COALESCE(EXCLUDED.time_specifier, contracts.time_specifier),
                    question = COALESCE(EXCLUDED.question, contracts.question),
                    conid_yes = COALESCE(EXCLUDED.conid_yes, contracts.conid_yes),
                    conid_no = COALESCE(EXCLUDED.conid_no, contracts.conid_no),
                    product_conid = COALESCE(EXCLUDED.product_conid, contracts.product_conid),
                    market_name = COALESCE(EXCLUDED.market_name, contracts.market_name),
                    symbol = COALESCE(EXCLUDED.symbol, contracts.symbol),
                    measured_period = COALESCE(EXCLUDED.measured_period, contracts.measured_period),
                    measured_period_units = COALESCE(EXCLUDED.measured_period_units, contracts.measured_period_units),
                    active = EXCLUDED.active,
                    last_seen_at = EXCLUDED.last_seen_at,
                    last_details_collected_at = COALESCE(EXCLUDED.last_details_collected_at, contracts.last_details_collected_at),
                    last_open_interest_collected_at = COALESCE(EXCLUDED.last_open_interest_collected_at, contracts.last_open_interest_collected_at),
                    last_history_collected_at = COALESCE(EXCLUDED.last_history_collected_at, contracts.last_history_collected_at),
                    last_history_no_data_at = COALESCE(EXCLUDED.last_history_no_data_at, contracts.last_history_no_data_at)
                """,
                params,
            )
        return len(params)

    def deactivate_missing_contracts(
        self,
        underlying_conid: int,
        active_conids: Sequence[int],
    ) -> None:
        if active_conids:
            self.conn.execute(
                """
                UPDATE contracts
                SET active = FALSE
                WHERE underlying_conid = %s
                  AND conid <> ALL(%s::bigint[])
                """,
                (underlying_conid, list(active_conids)),
            )
        else:
            self.conn.execute(
                "UPDATE contracts SET active = FALSE WHERE underlying_conid = %s",
                (underlying_conid,),
            )

    def list_contracts_for_underlying(
        self,
        underlying_conid: int,
        *,
        active_only: bool = True,
    ) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            f"""
            SELECT conid, underlying_conid, side, strike, strike_label, expiration, expiry_label,
                   time_specifier, question, conid_yes, conid_no, product_conid, market_name,
                   symbol, measured_period, measured_period_units, active, first_seen_at, last_seen_at,
                   last_details_collected_at, last_open_interest_collected_at,
                   last_history_collected_at, last_history_no_data_at
            FROM contracts
            WHERE underlying_conid = %s
              {"AND COALESCE(active, TRUE) = TRUE" if active_only else ""}
            ORDER BY conid
            """,
            (underlying_conid,),
        ).fetchall()
        return list(rows)

    def list_contracts(
        self,
        *,
        active_only: bool = True,
    ) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            f"""
            SELECT conid, underlying_conid, side, strike, strike_label, expiration, expiry_label,
                   time_specifier, question, conid_yes, conid_no, product_conid, market_name,
                   symbol, measured_period, measured_period_units, active, first_seen_at, last_seen_at,
                   last_details_collected_at, last_open_interest_collected_at,
                   last_history_collected_at, last_history_no_data_at
            FROM contracts
            WHERE 1 = 1
              {"AND COALESCE(active, TRUE) = TRUE" if active_only else ""}
            ORDER BY underlying_conid, conid
            """
        ).fetchall()
        return list(rows)

    def list_history_requests_for_incremental(
        self,
        periods: Sequence[str],
        *,
        limit: int,
        underlying_conid: int | None = None,
    ) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            """
            WITH desired_requests AS (
                SELECT
                    contracts.conid,
                    contracts.underlying_conid,
                    period_requested
                FROM contracts
                CROSS JOIN unnest(%s::text[]) AS periods(period_requested)
                WHERE COALESCE(contracts.active, TRUE) = TRUE
                  AND (%s::bigint IS NULL OR contracts.underlying_conid = %s::bigint)
            )
            SELECT
                desired_requests.conid,
                desired_requests.underlying_conid,
                desired_requests.period_requested,
                state.last_collected_at,
                state.last_no_data_at
            FROM desired_requests
            LEFT JOIN contract_history_collection_state AS state
              ON state.conid = desired_requests.conid
             AND state.period_requested = desired_requests.period_requested
            ORDER BY
                CASE WHEN state.last_collected_at IS NULL THEN 0 ELSE 1 END,
                COALESCE(state.last_collected_at, TIMESTAMPTZ 'epoch'),
                desired_requests.underlying_conid,
                desired_requests.conid,
                desired_requests.period_requested
            LIMIT %s
            """,
            (list(periods), underlying_conid, underlying_conid, limit),
        ).fetchall()
        return list(rows)

    def list_history_requests_for_backfill(
        self,
        periods: Sequence[str],
        *,
        limit: int,
        no_data_retry_before: datetime,
        underlying_conid: int | None = None,
    ) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            """
            WITH desired_requests AS (
                SELECT
                    contracts.conid,
                    contracts.underlying_conid,
                    period_requested
                FROM contracts
                CROSS JOIN unnest(%s::text[]) AS periods(period_requested)
                WHERE COALESCE(contracts.active, TRUE) = TRUE
                  AND (%s::bigint IS NULL OR contracts.underlying_conid = %s::bigint)
            )
            SELECT
                desired_requests.conid,
                desired_requests.underlying_conid,
                desired_requests.period_requested,
                state.last_collected_at,
                state.last_no_data_at
            FROM desired_requests
            LEFT JOIN contract_history_collection_state AS state
              ON state.conid = desired_requests.conid
             AND state.period_requested = desired_requests.period_requested
            WHERE state.last_collected_at IS NULL
               OR (
                    state.last_no_data_at IS NOT NULL
                AND state.last_no_data_at <= %s
               )
            ORDER BY
                CASE WHEN state.last_collected_at IS NULL THEN 0 ELSE 1 END,
                COALESCE(state.last_no_data_at, TIMESTAMPTZ 'epoch'),
                desired_requests.underlying_conid,
                desired_requests.conid,
                desired_requests.period_requested
            LIMIT %s
            """,
            (list(periods), underlying_conid, underlying_conid, no_data_retry_before, limit),
        ).fetchall()
        return list(rows)

    def insert_history_points(self, points: Sequence[HistoryPoint]) -> int:
        if not points:
            return 0
        params = [
            (
                point.conid,
                point.ts_utc,
                point.avg,
                point.volume,
                point.chart_step,
                point.source,
                point.period_requested,
                point.collected_at,
            )
            for point in points
        ]
        with self.conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO contract_history (
                    conid,
                    ts_utc,
                    avg,
                    volume,
                    chart_step,
                    source,
                    period_requested,
                    collected_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (conid, ts_utc, period_requested) DO UPDATE SET
                    avg = EXCLUDED.avg,
                    volume = EXCLUDED.volume,
                    chart_step = EXCLUDED.chart_step,
                    source = EXCLUDED.source,
                    collected_at = EXCLUDED.collected_at
                """,
                params,
            )
        return len(params)

    def mark_contract_history_collected(
        self,
        conid: int,
        collected_at: datetime,
        *,
        no_data: bool = False,
        period_requested: str | None = None,
    ) -> None:
        self.conn.execute(
            """
            UPDATE contracts
            SET last_history_collected_at = %s,
                last_history_no_data_at = CASE WHEN %s THEN %s ELSE NULL END
            WHERE conid = %s
            """,
            (collected_at, no_data, collected_at, conid),
        )
        if period_requested is not None:
            self.conn.execute(
                """
                INSERT INTO contract_history_collection_state (
                    conid,
                    period_requested,
                    last_collected_at,
                    last_no_data_at
                )
                VALUES (%s, %s, %s, CASE WHEN %s THEN %s ELSE NULL END)
                ON CONFLICT (conid, period_requested) DO UPDATE SET
                    last_collected_at = EXCLUDED.last_collected_at,
                    last_no_data_at = CASE
                        WHEN EXCLUDED.last_no_data_at IS NOT NULL THEN EXCLUDED.last_no_data_at
                        ELSE NULL
                    END
                """,
                (conid, period_requested, collected_at, no_data, collected_at),
            )

    def insert_open_interest_snapshot(self, snapshot: OpenInterestSnapshot) -> None:
        self.insert_open_interest_snapshots([snapshot])

    def insert_open_interest_snapshots(
        self,
        snapshots: Sequence[OpenInterestSnapshot],
    ) -> int:
        if not snapshots:
            return 0
        params = [
            (snapshot.conid, snapshot.open_interest, snapshot.collected_at)
            for snapshot in snapshots
        ]
        with self.conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO open_interest_snapshots (conid, open_interest, collected_at)
                VALUES (%s, %s, %s)
                """,
                params,
            )
        latest_by_conid: dict[int, datetime] = {}
        for snapshot in snapshots:
            previous = latest_by_conid.get(snapshot.conid)
            if previous is None or snapshot.collected_at > previous:
                latest_by_conid[snapshot.conid] = snapshot.collected_at
        with self.conn.cursor() as cur:
            cur.executemany(
                """
                UPDATE contracts
                SET last_open_interest_collected_at = %s
                WHERE conid = %s
                """,
                [(collected_at, conid) for conid, collected_at in latest_by_conid.items()],
            )
        return len(params)

    def insert_projected_probabilities(
        self,
        probabilities: Sequence[ProjectedProbability],
    ) -> int:
        if not probabilities:
            return 0
        params = [
            (
                probability.underlying_conid,
                probability.strike,
                probability.expiry,
                probability.probability,
                probability.collected_at,
            )
            for probability in probabilities
        ]
        with self.conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO projected_probabilities (
                    underlying_conid,
                    strike,
                    expiry,
                    probability,
                    collected_at
                )
                VALUES (%s, %s, %s, %s, %s)
                """,
                params,
            )
        return len(params)

    def mark_market_probabilities_collected(
        self,
        underlying_conid: int,
        collected_at: datetime,
    ) -> None:
        self.conn.execute(
            """
            UPDATE markets
            SET last_probabilities_collected_at = %s
            WHERE underlying_conid = %s
            """,
            (collected_at, underlying_conid),
        )

    def get_health_report(self) -> HealthReport:
        now = datetime.now(tz=UTC)
        window_start = now - timedelta(hours=24)

        market_counts = self.conn.execute(
            """
            SELECT
                COUNT(*)::bigint AS total_markets,
                COUNT(*) FILTER (WHERE COALESCE(active, TRUE) = TRUE)::bigint AS active_markets,
                COUNT(*) FILTER (WHERE COALESCE(active, TRUE) = FALSE)::bigint AS inactive_markets
            FROM markets
            """
        ).fetchone()
        new_contracts = self.conn.execute(
            """
            SELECT COUNT(*)::bigint AS count
            FROM contracts
            WHERE first_seen_at >= %s
            """,
            (window_start,),
        ).fetchone()
        failed_runs_rows = self.conn.execute(
            """
            SELECT job_name, COUNT(*)::bigint AS count
            FROM collection_runs
            WHERE status = 'failed'
              AND started_at >= %s
            GROUP BY job_name
            ORDER BY job_name
            """,
            (window_start,),
        ).fetchall()
        raw_response_rows = self.conn.execute(
            """
            SELECT endpoint_name, COUNT(*)::bigint AS count
            FROM raw_api_responses
            WHERE fetched_at >= %s
            GROUP BY endpoint_name
            ORDER BY endpoint_name
            """,
            (window_start,),
        ).fetchall()
        empty_probability_rows = self.conn.execute(
            """
            SELECT DISTINCT COALESCE((query_params->>'und_conid')::bigint, 0) AS underlying_conid
            FROM raw_api_responses
            WHERE endpoint_name = 'projected_probabilities'
              AND fetched_at >= %s
              AND jsonb_array_length(
                    COALESCE(
                        response_json->'projectedProbabilities',
                        response_json->'projected_probabilities',
                        '[]'::jsonb
                    )
                  ) = 0
            ORDER BY underlying_conid
            """,
            (window_start,),
        ).fetchall()
        no_data_rows = self.conn.execute(
            """
            SELECT DISTINCT COALESCE((query_params->>'conid')::bigint, 0) AS conid
            FROM raw_api_responses
            WHERE endpoint_name = 'history'
              AND fetched_at >= %s
              AND COALESCE((response_json->>'no_data')::boolean, FALSE) = TRUE
            ORDER BY conid
            """,
            (window_start,),
        ).fetchall()
        raw_response_size = self.conn.execute(
            "SELECT pg_total_relation_size('raw_api_responses')::bigint AS size_bytes"
        ).fetchone()

        failed_runs_by_job = {str(row["job_name"]): int(row["count"]) for row in failed_runs_rows}
        raw_responses_last_24h = {
            str(row["endpoint_name"]): int(row["count"]) for row in raw_response_rows
        }

        return HealthReport(
            generated_at=now,
            total_markets=int(market_counts["total_markets"]),
            active_markets=int(market_counts["active_markets"]),
            inactive_markets=int(market_counts["inactive_markets"]),
            new_contracts_last_24h=int(new_contracts["count"]),
            failed_runs_last_24h=sum(failed_runs_by_job.values()),
            failed_runs_by_job=failed_runs_by_job,
            raw_responses_last_24h=raw_responses_last_24h,
            empty_probability_markets_last_24h=[
                int(row["underlying_conid"])
                for row in empty_probability_rows
                if int(row["underlying_conid"]) != 0
            ],
            history_no_data_contracts_last_24h=[
                int(row["conid"]) for row in no_data_rows if int(row["conid"]) != 0
            ],
            raw_api_responses_disk_bytes=(
                int(raw_response_size["size_bytes"]) if raw_response_size else None
            ),
        )
