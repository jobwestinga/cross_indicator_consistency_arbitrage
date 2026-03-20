from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import dict_row

from .models import (
    ApiResponseEnvelope,
    ContractRecord,
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
        self._conn = psycopg.connect(self.database_url, row_factory=dict_row)
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
                "SELECT 1 FROM schema_migrations WHERE version = %s", (path.name,)
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

    def start_run(self, job_name: str) -> int:
        row = self.conn.execute(
            """
            INSERT INTO collection_runs (job_name, status)
            VALUES (%s, 'running')
            RETURNING id
            """,
            (job_name,),
        ).fetchone()
        self.conn.commit()
        return int(row["id"])

    def finish_run(self, run_id: int, status: str, error_text: str | None = None) -> None:
        self.conn.execute(
            """
            UPDATE collection_runs
            SET status = %s,
                error_text = %s,
                finished_at = NOW()
            WHERE id = %s
            """,
            (status, error_text, run_id),
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
        self.conn.commit()

    def upsert_market(self, market: MarketRecord) -> None:
        self.conn.execute(
            """
            INSERT INTO markets (
                underlying_conid,
                market_name,
                symbol,
                exchange,
                logo_category,
                payout,
                exclude_historical_data,
                first_seen_at,
                last_seen_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (underlying_conid) DO UPDATE SET
                market_name = EXCLUDED.market_name,
                symbol = EXCLUDED.symbol,
                exchange = EXCLUDED.exchange,
                logo_category = EXCLUDED.logo_category,
                payout = EXCLUDED.payout,
                exclude_historical_data = EXCLUDED.exclude_historical_data,
                last_seen_at = EXCLUDED.last_seen_at
            """,
            (
                market.underlying_conid,
                market.market_name,
                market.symbol,
                market.exchange,
                market.logo_category,
                market.payout,
                market.exclude_historical_data,
                market.first_seen_at,
                market.last_seen_at,
            ),
        )
        self.conn.commit()

    def upsert_contract(self, contract: ContractRecord) -> None:
        self.conn.execute(
            """
            INSERT INTO contracts (
                conid,
                underlying_conid,
                side,
                strike,
                strike_label,
                expiration,
                question,
                conid_yes,
                conid_no,
                product_conid,
                market_name,
                symbol,
                measured_period,
                measured_period_units,
                first_seen_at,
                last_seen_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (conid) DO UPDATE SET
                underlying_conid = EXCLUDED.underlying_conid,
                side = COALESCE(EXCLUDED.side, contracts.side),
                strike = COALESCE(EXCLUDED.strike, contracts.strike),
                strike_label = COALESCE(EXCLUDED.strike_label, contracts.strike_label),
                expiration = COALESCE(EXCLUDED.expiration, contracts.expiration),
                question = COALESCE(EXCLUDED.question, contracts.question),
                conid_yes = COALESCE(EXCLUDED.conid_yes, contracts.conid_yes),
                conid_no = COALESCE(EXCLUDED.conid_no, contracts.conid_no),
                product_conid = COALESCE(EXCLUDED.product_conid, contracts.product_conid),
                market_name = COALESCE(EXCLUDED.market_name, contracts.market_name),
                symbol = COALESCE(EXCLUDED.symbol, contracts.symbol),
                measured_period = COALESCE(EXCLUDED.measured_period, contracts.measured_period),
                measured_period_units = COALESCE(EXCLUDED.measured_period_units, contracts.measured_period_units),
                last_seen_at = EXCLUDED.last_seen_at
            """,
            (
                contract.conid,
                contract.underlying_conid,
                contract.side,
                contract.strike,
                contract.strike_label,
                contract.expiration,
                contract.question,
                contract.conid_yes,
                contract.conid_no,
                contract.product_conid,
                contract.market_name,
                contract.symbol,
                contract.measured_period,
                contract.measured_period_units,
                contract.first_seen_at,
                contract.last_seen_at,
            ),
        )
        self.conn.commit()

    def list_contracts_for_underlying(self, underlying_conid: int) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT conid, underlying_conid, side, strike, strike_label, expiration, question,
                   conid_yes, conid_no, product_conid, market_name, symbol,
                   measured_period, measured_period_units, first_seen_at, last_seen_at
            FROM contracts
            WHERE underlying_conid = %s
            ORDER BY conid
            """,
            (underlying_conid,),
        ).fetchall()
        return list(rows)

    def insert_history_points(self, points: list[HistoryPoint]) -> int:
        inserted = 0
        for point in points:
            self.conn.execute(
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
                ON CONFLICT (conid, ts_utc) DO UPDATE SET
                    avg = EXCLUDED.avg,
                    volume = EXCLUDED.volume,
                    chart_step = EXCLUDED.chart_step,
                    source = EXCLUDED.source,
                    period_requested = EXCLUDED.period_requested,
                    collected_at = EXCLUDED.collected_at
                """,
                (
                    point.conid,
                    point.ts_utc,
                    point.avg,
                    point.volume,
                    point.chart_step,
                    point.source,
                    point.period_requested,
                    point.collected_at,
                ),
            )
            inserted += 1
        self.conn.commit()
        return inserted

    def insert_open_interest_snapshot(self, snapshot: OpenInterestSnapshot) -> None:
        self.conn.execute(
            """
            INSERT INTO open_interest_snapshots (conid, open_interest, collected_at)
            VALUES (%s, %s, %s)
            """,
            (snapshot.conid, snapshot.open_interest, snapshot.collected_at),
        )
        self.conn.commit()

    def insert_projected_probabilities(self, probabilities: list[ProjectedProbability]) -> int:
        inserted = 0
        for probability in probabilities:
            self.conn.execute(
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
                (
                    probability.underlying_conid,
                    probability.strike,
                    probability.expiry,
                    probability.probability,
                    probability.collected_at,
                ),
            )
            inserted += 1
        self.conn.commit()
        return inserted
