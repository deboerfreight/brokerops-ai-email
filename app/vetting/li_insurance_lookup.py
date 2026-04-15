"""BrokerOps AI – FMCSA L&I bulk-file insurance lookup.

QCMobile's REST API returns None for insurance fields. FMCSA's L&I public
site is captcha-locked. But the FMCSA Data Dissemination Program publishes
the bulk L&I tables on datahub.transportation.gov as comma-delimited CSVs,
refreshed monthly.

This module:
  1. Loads the bulk Insur file (active insurance filings, keyed by docket)
     and Carrier file (docket <-> DOT mapping) into a local SQLite DB.
  2. Provides get_insurance(dot) -> InsurancePolicy with real BIPD/cargo
     amounts and insurer name, for the vetting pipeline to merge into the
     FMCSA normalized dict.

Canonical source URLs (as of 2026-04):
  - Insur:   https://datahub.transportation.gov/api/views/ypjt-5ydn/rows.csv?accessType=DOWNLOAD
  - Carrier: https://datahub.transportation.gov/api/views/6eyk-hxee/rows.csv?accessType=DOWNLOAD

File semantics:
  - The `Insur - All With History` file contains ONLY currently-active
    (non-cancelled) insurance filings. Despite "With History" in the name,
    cancelled policies are in the separate InsHist file.
  - max_cov_amount and BIPD_FILE are expressed in THOUSANDS of dollars
    (e.g. "01000" = $1,000,000).
  - Insur rows are keyed by prefix_docket_number (MC/FF/MX). The Carrier
    file provides docket <-> DOT mapping.
  - Cargo insurance filings (BMC-34) are only required for household-goods
    carriers. General-freight carriers typically have zero cargo rows in
    this file — that's expected, not missing data.

Insurance type/form codes:
  - ins_type_code=1 (BIPD liability, form 91X/91)
  - ins_type_code=2 (cargo, form 34)
  - ins_type_code=3 (broker/freight-forwarder bond, form 84/85)

Monthly refresh: run `scripts/refresh_li_insurance.py` on the 3rd Friday of
each month (after FMCSA publishes the new snapshot).
"""
from __future__ import annotations

import csv
import logging
import sqlite3
import time
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional, Sequence

logger = logging.getLogger("brokerops.vetting.li_insurance")

LI_DATA_DIR = Path(__file__).resolve().parents[2] / "data" / "fmcsa_li"
LI_DB_PATH = LI_DATA_DIR / "insurance_lookup.sqlite"

# Socrata datahub dataset IDs
DATASET_INSUR = "ypjt-5ydn"     # Insur - All With History  (active filings)
DATASET_CARRIER = "6eyk-hxee"   # Carrier - All With History (docket <-> DOT)

INSUR_CSV_URL = (
    f"https://datahub.transportation.gov/api/views/{DATASET_INSUR}/rows.csv?accessType=DOWNLOAD"
)
CARRIER_CSV_URL = (
    f"https://datahub.transportation.gov/api/views/{DATASET_CARRIER}/rows.csv?accessType=DOWNLOAD"
)


# ── Data class ────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class SourcingCandidate:
    """A candidate carrier from the L&I bulk sourcing index.

    Produced by `search_carriers_by_state()`. These rows come from the
    `Carrier - All With History` bulk file and are pre-filtered on state,
    ZIP prefix, BIPD amount on file, and authority status. Hand the DOT
    off to QCMobile (`get_carrier_details`) for full hydration before
    vetting.
    """
    dot: str                 # zero-padded 8-digit
    legal_name: str
    dba_name: str
    docket: str              # full MCxxxxxxx docket, if any
    bus_city: str
    bus_state: str
    bus_zip: str             # may include ZIP+4 (e.g. "33431-7304")
    bipd_filed: int          # dollars (not thousands)
    common_stat: str
    contract_stat: str
    broker_stat: str


@dataclass(frozen=True)
class InsurancePolicy:
    dot: str                    # zero-padded 8-digit
    bipd_liability: int         # dollars (not thousands)
    cargo: int                  # dollars; 0 if no filed cargo policy
    insurer_name: str           # name of the primary BIPD insurer (or cargo insurer if no BIPD)
    effective_date: str         # ISO YYYY-MM-DD of the primary policy
    expiration_date: str        # always "" — Insur file only holds active policies
    policy_type: str            # BIPD, CARGO, BIPD+CARGO, BOND, or MIXED
    source: str = "fmcsa_li_bulk"
    file_date: str = ""         # YYYYMMDD of the CSV snapshot


# ── Helpers ───────────────────────────────────────────────────────────────


def _pad_dot(dot: str | int) -> str:
    """Zero-pad a DOT number to 8 digits (FMCSA bulk-file convention)."""
    s = str(dot).strip()
    if not s:
        return ""
    # Tolerate "DOT 1234" style
    digits = "".join(c for c in s if c.isdigit())
    if not digits:
        return ""
    return digits.zfill(8)


def _parse_amount_thousands(raw: str) -> int:
    """Convert FMCSA thousands-encoded amount (e.g. '01000') to dollars."""
    if not raw:
        return 0
    s = str(raw).strip().lstrip("0") or "0"
    try:
        return int(s) * 1000
    except ValueError:
        return 0


def _parse_date(raw: str) -> str:
    """Convert MM/DD/YYYY to ISO YYYY-MM-DD (blank on failure)."""
    if not raw:
        return ""
    s = str(raw).strip()
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return ""


# ── Database builder ──────────────────────────────────────────────────────


_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS insurance (
    dot                TEXT PRIMARY KEY,
    bipd_liability     INTEGER NOT NULL,
    cargo              INTEGER NOT NULL,
    insurer_name       TEXT NOT NULL,
    effective_date     TEXT NOT NULL,
    policy_type        TEXT NOT NULL,
    file_date          TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_insurance_dot ON insurance(dot);

-- carriers_sourcing: denormalized lookup for the prospect pipeline.
-- One row per DOT, sourced from li_carrier_*.csv (Carrier - All With History).
-- Contains the minimum fields needed for state/ZIP/insurance pre-filtering
-- before hitting QCMobile for detail hydration.
CREATE TABLE IF NOT EXISTS carriers_sourcing (
    dot                TEXT PRIMARY KEY,
    legal_name         TEXT NOT NULL,
    dba_name           TEXT NOT NULL,
    docket             TEXT NOT NULL,
    bus_city           TEXT NOT NULL,
    bus_state          TEXT NOT NULL,
    bus_zip            TEXT NOT NULL,
    bus_zip5           TEXT NOT NULL,  -- first 5 digits for fast prefix match
    bipd_filed         INTEGER NOT NULL,  -- dollars, NOT thousands
    common_stat        TEXT NOT NULL,
    contract_stat      TEXT NOT NULL,
    broker_stat        TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_sourcing_state_zip
    ON carriers_sourcing(bus_state, bus_zip5);
CREATE INDEX IF NOT EXISTS idx_sourcing_state_bipd
    ON carriers_sourcing(bus_state, bipd_filed);
"""


def _find_latest_csv(pattern: str) -> Optional[Path]:
    """Return the most recent file matching the pattern in LI_DATA_DIR."""
    candidates = sorted(LI_DATA_DIR.glob(pattern), reverse=True)
    return candidates[0] if candidates else None


def build_lookup_db(
    insur_csv: Optional[Path] = None,
    carrier_csv: Optional[Path] = None,
    db_path: Optional[Path] = None,
) -> int:
    """One-time: load the Insur + Carrier CSVs into a SQLite database keyed by DOT.

    Consolidation (one row per DOT):
      - bipd_liability = max over all ins_type_code=1 (BIPD) policies
      - cargo          = max over all ins_type_code=2 (CARGO) policies
      - insurer_name   = insurer of the highest-BIPD policy (else highest-cargo
                         else highest-bond)
      - effective_date = latest effective_date across the policies folded in
      - policy_type    = composite label (BIPD, CARGO, BIPD+CARGO, BOND, MIXED)

    Idempotent — drops and rebuilds the `insurance` table.

    Returns number of DOTs indexed.
    """
    insur_csv = insur_csv or _find_latest_csv("li_insur_*.csv")
    carrier_csv = carrier_csv or _find_latest_csv("li_carrier_*.csv")
    db_path = db_path or LI_DB_PATH

    if not insur_csv or not insur_csv.exists():
        raise FileNotFoundError(
            f"Insur CSV not found. Run scripts/refresh_li_insurance.py first."
        )
    if not carrier_csv or not carrier_csv.exists():
        raise FileNotFoundError(
            f"Carrier CSV not found. Run scripts/refresh_li_insurance.py first."
        )

    # Extract file_date from filename (li_insur_YYYYMMDD.csv)
    file_date = ""
    try:
        file_date = insur_csv.stem.split("_")[-1]
    except Exception:
        file_date = datetime.utcnow().strftime("%Y%m%d")

    logger.info("Building L&I lookup DB from %s + %s", insur_csv.name, carrier_csv.name)
    t0 = time.time()

    # Step 1: read Carrier file -> docket -> (dot, legal_name, active?)
    docket_to_dot: dict[str, str] = {}
    active_carriers: set[str] = set()   # set of DOTs with any active authority
    with open(carrier_csv, "r", encoding="utf-8", errors="replace", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            docket = (row.get("DOCKET_NUMBER") or "").strip()
            dot = (row.get("DOT_NUMBER") or "").strip()
            if not docket or not dot or dot == "00000000":
                continue
            docket_to_dot[docket] = dot
            # Consider active if any of common/contract/broker is A
            if (
                row.get("COMMON_STAT") == "A"
                or row.get("CONTRACT_STAT") == "A"
                or row.get("BROKER_STAT") == "A"
            ):
                active_carriers.add(dot)
    logger.info(
        "Carrier index: %d dockets -> DOTs (%d DOTs with active authority) in %.1fs",
        len(docket_to_dot), len(active_carriers), time.time() - t0,
    )

    # Step 2: read Insur file, aggregate per DOT
    # per_dot[dot] = {"bipd": max_amount, "cargo": max_amount, "bond": max_amount,
    #                 "insurer": (best_amount_so_far, name), "eff": latest_iso}
    per_dot: dict[str, dict] = {}
    insur_rows = 0
    unmapped = 0
    t_insur = time.time()
    with open(insur_csv, "r", encoding="utf-8", errors="replace", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            insur_rows += 1
            docket = (row.get("prefix_docket_number") or "").strip()
            dot = docket_to_dot.get(docket)
            if not dot:
                unmapped += 1
                continue
            type_code = (row.get("ins_type_code") or "").strip()
            amount = _parse_amount_thousands(row.get("max_cov_amount") or "")
            eff_iso = _parse_date(row.get("effective_date") or "")
            insurer = (row.get("name_company") or "").strip()

            bucket = per_dot.setdefault(
                dot,
                {"bipd": 0, "cargo": 0, "bond": 0,
                 "bipd_insurer": "", "cargo_insurer": "", "bond_insurer": "",
                 "eff": ""},
            )
            if type_code == "1":  # BIPD
                if amount > bucket["bipd"]:
                    bucket["bipd"] = amount
                    bucket["bipd_insurer"] = insurer
            elif type_code == "2":  # Cargo
                if amount > bucket["cargo"]:
                    bucket["cargo"] = amount
                    bucket["cargo_insurer"] = insurer
            elif type_code == "3":  # Bond
                if amount > bucket["bond"]:
                    bucket["bond"] = amount
                    bucket["bond_insurer"] = insurer
            # latest eff date wins
            if eff_iso and eff_iso > bucket["eff"]:
                bucket["eff"] = eff_iso
    logger.info(
        "Insur index: %d rows -> %d DOTs (%d unmapped dockets) in %.1fs",
        insur_rows, len(per_dot), unmapped, time.time() - t_insur,
    )

    # Step 3: write SQLite
    LI_DATA_DIR.mkdir(parents=True, exist_ok=True)
    if db_path.exists():
        db_path.unlink()
    t_db = time.time()
    with closing(sqlite3.connect(db_path)) as conn:
        conn.executescript(_SCHEMA_SQL)
        conn.execute("DELETE FROM insurance")
        conn.execute("DELETE FROM meta")

        rows_to_insert = []
        for dot, b in per_dot.items():
            bipd = b["bipd"]
            cargo = b["cargo"]
            bond = b["bond"]
            # Primary insurer: BIPD > cargo > bond
            if bipd > 0:
                insurer = b["bipd_insurer"]
            elif cargo > 0:
                insurer = b["cargo_insurer"]
            else:
                insurer = b["bond_insurer"]

            ptype_parts = []
            if bipd > 0:
                ptype_parts.append("BIPD")
            if cargo > 0:
                ptype_parts.append("CARGO")
            if bond > 0:
                ptype_parts.append("BOND")
            if len(ptype_parts) == 0:
                continue  # skip dot with no amounts at all
            policy_type = "+".join(ptype_parts)

            rows_to_insert.append((
                dot, bipd, cargo, insurer, b["eff"], policy_type, file_date,
            ))

        conn.executemany(
            "INSERT INTO insurance (dot, bipd_liability, cargo, insurer_name, "
            "effective_date, policy_type, file_date) VALUES (?, ?, ?, ?, ?, ?, ?)",
            rows_to_insert,
        )
        conn.execute(
            "INSERT INTO meta (key, value) VALUES (?, ?)",
            ("built_at", datetime.utcnow().isoformat(timespec="seconds")),
        )
        conn.execute(
            "INSERT INTO meta (key, value) VALUES (?, ?)",
            ("file_date", file_date),
        )
        conn.execute(
            "INSERT INTO meta (key, value) VALUES (?, ?)",
            ("insur_csv", insur_csv.name),
        )
        conn.execute(
            "INSERT INTO meta (key, value) VALUES (?, ?)",
            ("carrier_csv", carrier_csv.name),
        )
        conn.execute(
            "INSERT INTO meta (key, value) VALUES (?, ?)",
            ("row_count", str(len(rows_to_insert))),
        )
        conn.commit()
    elapsed = time.time() - t0
    logger.info(
        "L&I SQLite built: %d DOTs indexed in %.1fs (db write %.1fs) -> %s",
        len(rows_to_insert), elapsed, time.time() - t_db, db_path,
    )
    return len(rows_to_insert)


# ── Sourcing index builder ───────────────────────────────────────────────


def _parse_bipd_thousands(raw: str) -> int:
    """Convert BIPD_FILE thousands-encoded amount (e.g. '01000') to dollars.

    Same semantics as `_parse_amount_thousands` but tolerant of blanks.
    BIPD_FILE is always 5 digits, zero-padded.
    """
    if not raw:
        return 0
    s = str(raw).strip().lstrip("0") or "0"
    try:
        return int(s) * 1000
    except ValueError:
        return 0


def _zip5(raw: str) -> str:
    """Extract first 5 numeric digits of a ZIP (strips ZIP+4 and spaces)."""
    if not raw:
        return ""
    digits = "".join(c for c in str(raw).strip() if c.isdigit())
    return digits[:5]


def build_sourcing_index(
    carrier_csv: Optional[Path] = None,
    db_path: Optional[Path] = None,
) -> int:
    """Populate the `carriers_sourcing` table from the L&I Carrier bulk file.

    Idempotent: drops and rebuilds the table each run. Safe to call against
    an existing insurance_lookup.sqlite that already has the `insurance`
    table — the two are independent.

    Filters applied at ingest:
      - US country only (BUS_CTRY_CODE = 'US')
      - Valid DOT (non-zero, non-blank)
      - Has a US state code

    No authority/insurance filtering at build time — the query function
    decides what to surface based on caller params.

    Returns number of DOTs indexed.
    """
    carrier_csv = carrier_csv or _find_latest_csv("li_carrier_*.csv")
    db_path = db_path or LI_DB_PATH

    if not carrier_csv or not carrier_csv.exists():
        raise FileNotFoundError(
            f"Carrier CSV not found at {LI_DATA_DIR}. "
            "Run scripts/refresh_li_insurance.py first."
        )

    # Extract file_date from filename
    try:
        file_date = carrier_csv.stem.split("_")[-1]
    except Exception:
        file_date = datetime.utcnow().strftime("%Y%m%d")

    logger.info("Building carriers_sourcing index from %s", carrier_csv.name)
    t0 = time.time()

    LI_DATA_DIR.mkdir(parents=True, exist_ok=True)

    with closing(sqlite3.connect(db_path)) as conn:
        # Ensure base schema (insurance table may already exist; that's fine)
        conn.executescript(_SCHEMA_SQL)
        conn.execute("DELETE FROM carriers_sourcing")

        rows_to_insert: list[tuple] = []
        seen_dots: set[str] = set()
        skipped_foreign = 0
        skipped_no_dot = 0

        with open(
            carrier_csv, "r", encoding="utf-8", errors="replace", newline=""
        ) as f:
            reader = csv.DictReader(f)
            for row in reader:
                ctry = (row.get("BUS_CTRY_CODE") or "").strip().upper()
                if ctry and ctry != "US":
                    skipped_foreign += 1
                    continue

                dot_raw = (row.get("DOT_NUMBER") or "").strip()
                dot = _pad_dot(dot_raw)
                if not dot or dot == "00000000":
                    skipped_no_dot += 1
                    continue
                # Carrier file can have multiple rows per DOT (split auths).
                # Keep the first occurrence; the bulk file is sorted such that
                # the primary (MC) docket tends to come first.
                if dot in seen_dots:
                    continue
                seen_dots.add(dot)

                state = (row.get("BUS_STATE_CODE") or "").strip().upper()
                zip_raw = (row.get("BUS_ZIP_CODE") or "").strip()

                rows_to_insert.append((
                    dot,
                    (row.get("LEGAL_NAME") or "").strip(),
                    (row.get("DBA_NAME") or "").strip(),
                    (row.get("DOCKET_NUMBER") or "").strip(),
                    (row.get("BUS_CITY") or "").strip(),
                    state,
                    zip_raw,
                    _zip5(zip_raw),
                    _parse_bipd_thousands(row.get("BIPD_FILE") or ""),
                    (row.get("COMMON_STAT") or "").strip().upper(),
                    (row.get("CONTRACT_STAT") or "").strip().upper(),
                    (row.get("BROKER_STAT") or "").strip().upper(),
                ))

        t_write = time.time()
        conn.executemany(
            "INSERT INTO carriers_sourcing (dot, legal_name, dba_name, docket, "
            "bus_city, bus_state, bus_zip, bus_zip5, bipd_filed, common_stat, "
            "contract_stat, broker_stat) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            rows_to_insert,
        )

        # Stamp meta
        conn.execute(
            "INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)",
            ("sourcing_built_at", datetime.utcnow().isoformat(timespec="seconds")),
        )
        conn.execute(
            "INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)",
            ("sourcing_carrier_csv", carrier_csv.name),
        )
        conn.execute(
            "INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)",
            ("sourcing_row_count", str(len(rows_to_insert))),
        )
        conn.execute(
            "INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)",
            ("sourcing_file_date", file_date),
        )
        conn.commit()

    elapsed = time.time() - t0
    logger.info(
        "carriers_sourcing built: %d DOTs indexed in %.1fs "
        "(write %.1fs, skipped %d foreign, %d no-DOT) -> %s",
        len(rows_to_insert), elapsed, time.time() - t_write,
        skipped_foreign, skipped_no_dot, db_path,
    )
    return len(rows_to_insert)


# ── Sourcing query ───────────────────────────────────────────────────────


def _ensure_sourcing_index() -> None:
    """Raise if the sourcing index hasn't been built yet."""
    if not LI_DB_PATH.exists():
        raise RuntimeError(
            f"L&I DB not found at {LI_DB_PATH}. Run build_sourcing_index() first."
        )
    with closing(sqlite3.connect(LI_DB_PATH)) as conn:
        try:
            cnt = conn.execute(
                "SELECT COUNT(*) FROM carriers_sourcing"
            ).fetchone()[0]
        except sqlite3.OperationalError as exc:
            raise RuntimeError(
                f"carriers_sourcing table missing from {LI_DB_PATH}. "
                "Run build_sourcing_index() to populate it."
            ) from exc
        if cnt == 0:
            raise RuntimeError(
                "carriers_sourcing table is empty. "
                "Run build_sourcing_index() to populate it."
            )


def search_carriers_by_state(
    state: str,
    zip_prefixes: Optional[Sequence[str]] = None,
    min_bipd: int = 1_000_000,
    exclude_broker_only: bool = True,
    require_active_authority: bool = True,
    limit: int = 100,
) -> list[SourcingCandidate]:
    """Query the L&I bulk data for carriers matching geographic + insurance criteria.

    Returns candidate DOTs ready for QCMobile hydration. Fast: local SQLite
    query, typically <100ms for a state-wide search with ZIP filter.

    Filter rules:
      - state: 2-letter US state code (FL, GA, etc.); case-insensitive
      - zip_prefixes: if provided, only return carriers whose bus_zip5 starts
        with any prefix in the list (e.g. ["330","331"] for South FL)
      - min_bipd: minimum BIPD amount on file (dollars, NOT thousands)
      - exclude_broker_only: if True, skip carriers whose BROKER_STAT='A' AND
        COMMON_STAT != 'A' AND CONTRACT_STAT != 'A' (broker-only entities).
        Carriers with both motor and broker authority are kept.
      - require_active_authority: if True, require COMMON_STAT='A' OR
        CONTRACT_STAT='A' (at least one motor-carrier authority active).
      - limit: max rows returned

    Ordering: by bus_zip5 ASC, then legal_name ASC. Deterministic so a
    re-run with the same params yields the same candidate order (idempotent
    dedup via prospect_carriers.seen_dots).
    """
    _ensure_sourcing_index()

    state_norm = (state or "").strip().upper()
    if not state_norm:
        return []

    where = ["bus_state = ?"]
    params: list = [state_norm]

    if require_active_authority:
        where.append("(common_stat = 'A' OR contract_stat = 'A')")

    if exclude_broker_only:
        # Reject carriers that ONLY hold broker authority
        where.append(
            "NOT (broker_stat = 'A' AND common_stat != 'A' AND contract_stat != 'A')"
        )

    if min_bipd > 0:
        where.append("bipd_filed >= ?")
        params.append(int(min_bipd))

    if zip_prefixes:
        # SQLite has no IN-prefix operator; use OR-of-LIKEs or IN over substrs.
        # We stored bus_zip5 already, so a simple IN() on substr(zip5, 1, len(p))
        # only works when all prefixes have the same length. Fall back to OR.
        prefix_clauses = []
        for p in zip_prefixes:
            p = str(p).strip()
            if not p:
                continue
            prefix_clauses.append("bus_zip5 LIKE ?")
            params.append(f"{p}%")
        if prefix_clauses:
            where.append("(" + " OR ".join(prefix_clauses) + ")")

    sql = (
        "SELECT dot, legal_name, dba_name, docket, bus_city, bus_state, "
        "bus_zip, bipd_filed, common_stat, contract_stat, broker_stat "
        "FROM carriers_sourcing "
        "WHERE " + " AND ".join(where) + " "
        "ORDER BY bus_zip5 ASC, legal_name ASC "
        "LIMIT ?"
    )
    params.append(int(limit))

    with closing(sqlite3.connect(LI_DB_PATH)) as conn:
        rows = conn.execute(sql, params).fetchall()

    return [
        SourcingCandidate(
            dot=row[0],
            legal_name=row[1],
            dba_name=row[2],
            docket=row[3],
            bus_city=row[4],
            bus_state=row[5],
            bus_zip=row[6],
            bipd_filed=int(row[7]),
            common_stat=row[8],
            contract_stat=row[9],
            broker_stat=row[10],
        )
        for row in rows
    ]


# ── Public lookup ────────────────────────────────────────────────────────


def get_insurance(dot: str) -> Optional[InsurancePolicy]:
    """Query the local SQLite lookup for a DOT number.

    Returns None if the DOT has no active filing in the L&I bulk file, or
    the database hasn't been built yet (caller should log & fall through).
    """
    if not LI_DB_PATH.exists():
        logger.debug("L&I DB not built yet (expected at %s)", LI_DB_PATH)
        return None
    padded = _pad_dot(dot)
    if not padded:
        return None

    try:
        with closing(sqlite3.connect(LI_DB_PATH)) as conn:
            row = conn.execute(
                "SELECT dot, bipd_liability, cargo, insurer_name, "
                "effective_date, policy_type, file_date "
                "FROM insurance WHERE dot = ?",
                (padded,),
            ).fetchone()
    except sqlite3.Error as exc:
        logger.warning("L&I lookup DB error for DOT %s: %s", dot, exc)
        return None

    if not row:
        return None
    return InsurancePolicy(
        dot=row[0],
        bipd_liability=int(row[1]),
        cargo=int(row[2]),
        insurer_name=row[3],
        effective_date=row[4],
        expiration_date="",
        policy_type=row[5],
        file_date=row[6],
    )


def lookup_db_last_built() -> Optional[str]:
    """Return the YYYY-MM-DD of the last successful CSV import, or None."""
    if not LI_DB_PATH.exists():
        return None
    try:
        with closing(sqlite3.connect(LI_DB_PATH)) as conn:
            row = conn.execute(
                "SELECT value FROM meta WHERE key = 'file_date'"
            ).fetchone()
            if row and row[0]:
                raw = row[0]
                if len(raw) == 8 and raw.isdigit():
                    return f"{raw[:4]}-{raw[4:6]}-{raw[6:8]}"
                return raw
    except sqlite3.Error:
        return None
    return None


def lookup_db_stats() -> dict:
    """Return meta + row count for the current DB (or {} if none)."""
    if not LI_DB_PATH.exists():
        return {}
    try:
        with closing(sqlite3.connect(LI_DB_PATH)) as conn:
            meta = dict(conn.execute("SELECT key, value FROM meta").fetchall())
            count = conn.execute("SELECT COUNT(*) FROM insurance").fetchone()[0]
            meta["indexed_dots"] = count
            return meta
    except sqlite3.Error:
        return {}
