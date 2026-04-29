from __future__ import annotations

import io
import json
import sqlite3
import zipfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from .models import DatasetExportFile, DatasetExportSummary

if TYPE_CHECKING:
    from .repository import CollectorRepository


@dataclass(frozen=True)
class ExportSpec:
    key: str
    archive_name: str
    query: str
    params: tuple[Any, ...]


class DatasetExportService:
    def __init__(self, repository: CollectorRepository) -> None:
        self.repository = repository

    def export(
        self,
        output_dir: Path,
        *,
        dataset_name: str | None = None,
        underlying_conid: int | None = None,
        since: datetime | None = None,
    ) -> DatasetExportSummary:
        generated_at = datetime.now(tz=UTC)
        output_dir.mkdir(parents=True, exist_ok=True)

        if dataset_name is None:
            dataset_name = f"forecast_analysis_dataset_{generated_at:%Y%m%dT%H%M%SZ}.zip"
        elif not dataset_name.endswith(".zip"):
            dataset_name = f"{dataset_name}.zip"

        bundle_path = output_dir / dataset_name
        specs = self._build_specs(underlying_conid=underlying_conid, since=since)
        exported_files: list[DatasetExportFile] = []

        with zipfile.ZipFile(bundle_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for spec in specs:
                with archive.open(spec.archive_name, "w", force_zip64=True) as raw_file:
                    with io.TextIOWrapper(raw_file, encoding="utf-8", newline="") as text_file:
                        row_count = self.repository.write_query_csv(
                            spec.key,
                            spec.query,
                            text_file,
                            spec.params,
                        )
                exported_files.append(DatasetExportFile(name=spec.archive_name, rows=row_count))

            manifest = {
                "generated_at": generated_at.isoformat(),
                "underlying_conid": underlying_conid,
                "since": since.isoformat() if since is not None else None,
                "files": [item.model_dump() for item in exported_files],
            }
            archive.writestr("manifest.json", json.dumps(manifest, indent=2))

        total_rows = sum(item.rows for item in exported_files)
        return DatasetExportSummary(
            bundle_path=str(bundle_path),
            generated_at=generated_at,
            underlying_conid=underlying_conid,
            since=since,
            files=exported_files,
            message=(
                f"Exported {total_rows} rows across {len(exported_files)} files "
                f"to {bundle_path}"
            ),
        )

    def export_sqlite(
        self,
        output_dir: Path,
        *,
        dataset_name: str | None = None,
        underlying_conid: int | None = None,
        since: datetime | None = None,
    ) -> DatasetExportSummary:
        generated_at = datetime.now(tz=UTC)
        output_dir.mkdir(parents=True, exist_ok=True)

        if dataset_name is None:
            dataset_name = f"forecast_analysis_dataset_{generated_at:%Y%m%dT%H%M%SZ}.sqlite"
        elif not dataset_name.endswith(".sqlite"):
            dataset_name = f"{dataset_name}.sqlite"

        bundle_path = output_dir / dataset_name
        if bundle_path.exists():
            bundle_path.unlink()

        specs = self._build_specs(underlying_conid=underlying_conid, since=since)
        exported_files: list[DatasetExportFile] = []

        with sqlite3.connect(bundle_path) as sqlite_conn:
            for spec in specs:
                row_count = self.repository.write_query_sqlite(
                    spec.key,
                    spec.query,
                    sqlite_conn,
                    spec.key,
                    spec.params,
                )
                exported_files.append(DatasetExportFile(name=spec.key, rows=row_count))

            sqlite_conn.execute("DROP TABLE IF EXISTS export_manifest")
            sqlite_conn.execute(
                """
                CREATE TABLE export_manifest (
                    generated_at TEXT NOT NULL,
                    underlying_conid INTEGER,
                    since TEXT
                )
                """
            )
            sqlite_conn.execute(
                """
                INSERT INTO export_manifest (generated_at, underlying_conid, since)
                VALUES (?, ?, ?)
                """,
                (
                    generated_at.isoformat(),
                    underlying_conid,
                    since.isoformat() if since is not None else None,
                ),
            )
            sqlite_conn.execute("DROP TABLE IF EXISTS export_tables")
            sqlite_conn.execute(
                """
                CREATE TABLE export_tables (
                    table_name TEXT NOT NULL,
                    rows INTEGER NOT NULL
                )
                """
            )
            sqlite_conn.executemany(
                """
                INSERT INTO export_tables (table_name, rows)
                VALUES (?, ?)
                """,
                [(item.name, item.rows) for item in exported_files],
            )
            sqlite_conn.commit()

        total_rows = sum(item.rows for item in exported_files)
        return DatasetExportSummary(
            bundle_path=str(bundle_path),
            generated_at=generated_at,
            underlying_conid=underlying_conid,
            since=since,
            files=exported_files,
            message=(
                f"Exported {total_rows} rows across {len(exported_files)} tables "
                f"to {bundle_path}"
            ),
        )

    def _build_specs(
        self,
        *,
        underlying_conid: int | None,
        since: datetime | None,
    ) -> list[ExportSpec]:
        return [
            ExportSpec(
                key="market_categories",
                archive_name="market_categories.csv",
                query="""
                SELECT
                    mc.category_key,
                    mc.category_name,
                    mc.parent_category_key,
                    mc.first_seen_at,
                    mc.last_seen_at
                FROM market_categories AS mc
                WHERE (%s::bigint IS NULL OR EXISTS (
                    SELECT 1
                    FROM markets AS m
                    WHERE m.category_key = mc.category_key
                      AND m.underlying_conid = %s::bigint
                ))
                ORDER BY mc.category_key
                """,
                params=(underlying_conid, underlying_conid),
            ),
            ExportSpec(
                key="markets",
                archive_name="markets.csv",
                query="""
                SELECT
                    m.underlying_conid,
                    m.market_name,
                    m.symbol,
                    m.exchange,
                    m.product_conid,
                    m.category_key,
                    m.logo_category,
                    m.payout,
                    m.exclude_historical_data,
                    m.active,
                    m.first_seen_at,
                    m.last_seen_at,
                    m.last_discovered_at,
                    m.last_structure_collected_at,
                    m.last_probabilities_collected_at
                FROM markets AS m
                WHERE (%s::bigint IS NULL OR m.underlying_conid = %s::bigint)
                ORDER BY m.underlying_conid
                """,
                params=(underlying_conid, underlying_conid),
            ),
            ExportSpec(
                key="contracts",
                archive_name="contracts.csv",
                query="""
                SELECT
                    c.underlying_conid,
                    m.market_name,
                    m.symbol AS market_symbol,
                    c.conid,
                    c.side,
                    c.strike,
                    c.strike_label,
                    c.expiration,
                    c.expiry_label,
                    c.time_specifier,
                    c.question,
                    c.conid_yes,
                    c.conid_no,
                    c.product_conid,
                    c.market_name AS contract_market_name,
                    c.symbol AS contract_symbol,
                    c.measured_period,
                    c.measured_period_units,
                    c.active,
                    c.first_seen_at,
                    c.last_seen_at,
                    c.last_details_collected_at,
                    c.last_open_interest_collected_at,
                    c.last_history_collected_at,
                    c.last_history_no_data_at
                FROM contracts AS c
                JOIN markets AS m
                  ON m.underlying_conid = c.underlying_conid
                WHERE (%s::bigint IS NULL OR c.underlying_conid = %s::bigint)
                ORDER BY c.underlying_conid, c.conid
                """,
                params=(underlying_conid, underlying_conid),
            ),
            ExportSpec(
                key="projected_probabilities",
                archive_name="projected_probabilities.csv",
                query="""
                SELECT
                    p.underlying_conid,
                    m.market_name,
                    m.symbol AS market_symbol,
                    m.category_key,
                    p.strike,
                    p.expiry,
                    p.probability,
                    p.collected_at
                FROM projected_probabilities AS p
                JOIN markets AS m
                  ON m.underlying_conid = p.underlying_conid
                WHERE (%s::bigint IS NULL OR p.underlying_conid = %s::bigint)
                  AND (%s::timestamptz IS NULL OR p.collected_at >= %s::timestamptz)
                ORDER BY p.underlying_conid, p.collected_at, p.expiry, p.strike
                """,
                params=(underlying_conid, underlying_conid, since, since),
            ),
            ExportSpec(
                key="open_interest_snapshots",
                archive_name="open_interest_snapshots.csv",
                query="""
                SELECT
                    c.underlying_conid,
                    m.market_name,
                    m.symbol AS market_symbol,
                    c.conid,
                    c.side,
                    c.strike,
                    c.strike_label,
                    c.expiration,
                    c.expiry_label,
                    c.question,
                    s.open_interest,
                    s.collected_at
                FROM open_interest_snapshots AS s
                JOIN contracts AS c
                  ON c.conid = s.conid
                JOIN markets AS m
                  ON m.underlying_conid = c.underlying_conid
                WHERE (%s::bigint IS NULL OR c.underlying_conid = %s::bigint)
                  AND (%s::timestamptz IS NULL OR s.collected_at >= %s::timestamptz)
                ORDER BY c.underlying_conid, c.conid, s.collected_at
                """,
                params=(underlying_conid, underlying_conid, since, since),
            ),
            ExportSpec(
                key="contract_history",
                archive_name="contract_history.csv",
                query="""
                SELECT
                    c.underlying_conid,
                    m.market_name,
                    m.symbol AS market_symbol,
                    c.conid,
                    c.side,
                    c.strike,
                    c.strike_label,
                    c.expiration,
                    c.expiry_label,
                    c.question,
                    h.period_requested,
                    h.ts_utc,
                    h.avg,
                    h.volume,
                    h.chart_step,
                    h.source,
                    h.collected_at
                FROM contract_history AS h
                JOIN contracts AS c
                  ON c.conid = h.conid
                JOIN markets AS m
                  ON m.underlying_conid = c.underlying_conid
                WHERE (%s::bigint IS NULL OR c.underlying_conid = %s::bigint)
                  AND (%s::timestamptz IS NULL OR h.ts_utc >= %s::timestamptz)
                ORDER BY c.underlying_conid, c.conid, h.period_requested, h.ts_utc
                """,
                params=(underlying_conid, underlying_conid, since, since),
            ),
        ]
