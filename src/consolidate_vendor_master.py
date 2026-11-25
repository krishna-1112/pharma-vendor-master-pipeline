###############################################################################
# Script Name: consolidate_vendor_master.py
# Objective: Consolidate the ERP vendor master and the manual procurement log
#            into a clean Golden Vendor Master in SQLite: standardize, parse,
#            quarantine invalid records, impute, deduplicate and normalize
#            material categories
# Created By: Krishna
# Created Date: 24-11-2026
# Modified Date: 25-11-2026
# Modified By: Krishna
# Comments: All file paths are read from config/config.ini (copy it from
#           config/config.example.ini). The database and the three target
#           tables are created automatically from sql/ddl_schema.sql.
#           Run from the project root: python src/consolidate_vendor_master.py
###############################################################################

"""
Pseudocode :
# Step 1 : Read the file paths from config.ini and create the timestamped log file.
# Step 2 : Load both CSV files, tag each row with source_system (ERP / MANUAL_LOG) and combine them.
# Step 3 : Trim spaces, make vendor_id uppercase, Title Case vendor names and standardize countries.
# Step 4 : Clean contract values ($, commas, USD) into numbers and parse dates into ISO format.
# Step 5 : Move invalid records (blank name, bad contract value, negative lead time) to quarantine.
# Step 6 : Fill missing lead times with the median, flag them and keep the latest record per vendor.
# Step 7 : Split material categories and explode them into one row per vendor and category.
# Step 8 : Create the SQLite database and the three target tables from the DDL file.
# Step 9 : Load all three tables in one transaction (commit, or rollback on error).
# Step 10: Export the quarantine report CSV and run the verification queries.
# Step 11: Log the execution summary with the final status.

"""

import configparser
import io
import logging
import os
import sqlite3
import sys
from datetime import datetime

import numpy as np
import pandas as pd

# The script lives in src/, so the project root is one folder up
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_PATH = os.path.join(BASE_DIR, "config", "config.ini")
CONFIG_TEMPLATE = os.path.join(BASE_DIR, "config", "config.example.ini")
SCRIPT_NAME = os.path.splitext(os.path.basename(__file__))[0]

# ==============================================================================
# PIPELINE CONSTANTS (business rules from the task document)
# ==============================================================================
COUNTRY_MAPPING = {
    "INDIA": "India",
    "india": "India",
    "USA": "USA",
    "United States": "USA",
    "germany": "Germany",
    "uk": "UK",
    "UK": "UK",
}

# onboarded_date formats, tried in this order (DD-MM-YYYY must be day-first: 10-01-2025 = 10 Jan 2025)
DATE_FORMATS = ["%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d", "%d-%m-%Y"]

SOURCE_COLUMNS = [
    "vendor_id", "vendor_name", "country", "onboarded_date", "lead_time_days",
    "contract_value_usd", "material_categories", "last_modified",
]
MASTER_COLUMNS = [
    "vendor_id", "vendor_name", "country", "onboarded_date", "lead_time_days",
    "is_lead_time_imputed", "contract_value_usd", "source_system", "last_modified", "processed_at",
]
QUARANTINE_COLUMNS = [
    "vendor_id", "vendor_name", "country", "onboarded_date", "lead_time_days",
    "contract_value_usd", "material_categories", "source_system", "last_modified",
    "rejection_reason", "processed_at",
]

log = logging.getLogger(SCRIPT_NAME)

# Counters shown in the summary
stats = {
    "erp_rows": 0, "manual_rows": 0, "quarantined": 0, "valid": 0, "imputed": 0,
    "median_lead_time": None, "duplicates_removed": 0, "master_rows": 0,
    "category_rows": 0, "quarantine_rows": 0, "total_spend": None,
}


# --------------------------------------------------------------------------- #
# Setup
# --------------------------------------------------------------------------- #
def load_config():
    """Read config/config.ini (the local copy of config/config.example.ini, ignored by Git)."""
    config = configparser.ConfigParser(interpolation=None)
    if not config.read(CONFIG_PATH):
        raise FileNotFoundError(
            f"{CONFIG_PATH} not found. Create it from the template: "
            f"cp config/config.example.ini config/config.ini"
        )
    return config


def resolve_path(path):
    """Paths in config.ini are relative to the project root folder."""
    return path if os.path.isabs(path) else os.path.join(BASE_DIR, path)


def setup_logging(log_dir):
    """Log to the console and to <Scriptname_Timestamp.log>."""
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(
        log_dir, f"{SCRIPT_NAME}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    )
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-7s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.FileHandler(log_file, encoding="utf-8"), logging.StreamHandler()],
    )
    return log_file


def log_frame(df, columns=None):
    """Write a DataFrame (or some of its columns) into the log as a table."""
    view = df if columns is None else df[columns]
    for line in view.to_string(index=False).splitlines():
        log.info(f"        {line}")


# --------------------------------------------------------------------------- #
# Step 2: Ingestion & source lineage tagging
# --------------------------------------------------------------------------- #
def load_raw_sources(erp_path: str, manual_path: str) -> pd.DataFrame:
    """Ingest both source CSVs, add the source_system audit column and combine them."""
    for path in (erp_path, manual_path):
        if not os.path.isfile(path):
            raise FileNotFoundError(f"Source file not found: {path}")

    # dtype=str keeps every value exactly as written (e.g. '$240,500.75', '15/03/2024')
    df_erp = pd.read_csv(erp_path, dtype=str)
    df_erp["source_system"] = "ERP"
    df_manual = pd.read_csv(manual_path, dtype=str)
    df_manual["source_system"] = "MANUAL_LOG"

    for name, frame in (("ERP", df_erp), ("MANUAL_LOG", df_manual)):
        missing = [col for col in SOURCE_COLUMNS if col not in frame.columns]
        if missing:
            raise ValueError(f"{name} file is missing columns: {', '.join(missing)}")

    stats["erp_rows"], stats["manual_rows"] = len(df_erp), len(df_manual)
    log.info(f"    ERP rows read: {len(df_erp)}, MANUAL_LOG rows read: {len(df_manual)}")

    return pd.concat([df_erp, df_manual], ignore_index=True)


# --------------------------------------------------------------------------- #
# Step 3: String & country standardization
# --------------------------------------------------------------------------- #
def smart_title_case(name):
    """
    Title Case only names typed in all lower or all UPPER case:
    'apex pharma ingredients ltd' -> 'Apex Pharma Ingredients Ltd',
    'BIOSYNTH GLOBAL LOGISTICS'   -> 'Biosynth Global Logistics'.
    Names already in mixed case keep their brand casing ('SterileFill', 'MediPack').
    """
    if pd.isna(name):
        return name
    return name.title() if (name.islower() or name.isupper()) else name


def standardize_strings_and_countries(df: pd.DataFrame) -> pd.DataFrame:
    """Clean whitespace, standardize casing and normalize country names."""
    df = df.copy()

    # Strip leading/trailing whitespace in every text column; empty text becomes NaN (missing)
    for col in df.columns:
        df[col] = df[col].str.strip().replace("", np.nan)

    df["vendor_id"] = df["vendor_id"].str.upper()
    df["vendor_name"] = df["vendor_name"].apply(smart_title_case)

    # Map country aliases; values not in the dictionary keep their original spelling
    original_country = df["country"]
    df["country"] = df["country"].map(COUNTRY_MAPPING).fillna(original_country)

    changed = (original_country != df["country"]) & original_country.notna()
    for before, after in sorted(set(zip(original_country[changed], df["country"][changed]))):
        log.info(f"    Country mapped: '{before}' -> '{after}'")
    return df


# --------------------------------------------------------------------------- #
# Step 4: Numeric & date parsing
# --------------------------------------------------------------------------- #
def parse_mixed_dates(series: pd.Series) -> pd.Series:
    """Try each known format in order; the first format that fits wins."""
    parsed = pd.Series(pd.NaT, index=series.index, dtype="datetime64[ns]")
    for fmt in DATE_FORMATS:
        attempt = pd.to_datetime(series, format=fmt, errors="coerce")
        parsed = parsed.fillna(attempt)
    return parsed


def parse_types_and_dates(df: pd.DataFrame) -> pd.DataFrame:
    """Parse currency values, lead times and mixed date formats."""
    df = df.copy()

    # Keep the original text for the quarantine table (its contract_value_usd column is TEXT)
    df["contract_value_raw"] = df["contract_value_usd"]
    df["lead_time_raw"] = df["lead_time_days"]

    # '$240,500.75' -> 240500.75 | '245000 USD' -> 245000 | 'TBD' -> NaN
    cleaned = (df["contract_value_usd"]
               .str.replace(r"[$,]", "", regex=True)
               .str.replace(r"USD", "", case=False, regex=True)
               .str.strip())
    df["contract_value_usd"] = pd.to_numeric(cleaned, errors="coerce").astype("float64")

    df["lead_time_days"] = pd.to_numeric(df["lead_time_days"], errors="coerce").astype("float64")

    df["onboarded_date"] = parse_mixed_dates(df["onboarded_date"])
    df["last_modified"] = pd.to_datetime(df["last_modified"], errors="coerce").astype("datetime64[ns]")

    for _, row in df.iterrows():
        log.info(f"    {row['vendor_id']} ({row['source_system']}): contract '{row['contract_value_raw']}' -> "
                 f"{row['contract_value_usd']}, onboarded -> "
                 f"{row['onboarded_date'].date() if pd.notna(row['onboarded_date']) else 'UNPARSEABLE'}")
    return df


# --------------------------------------------------------------------------- #
# Step 5: Quarantine split (data quality gate)
# --------------------------------------------------------------------------- #
def split_quarantine_records(df: pd.DataFrame):
    """Check every row against the DQ rules. Returns (df_clean, df_quarantine)."""
    # Rules from the task document, plus safety nets for NOT NULL / primary key columns.
    # If a row breaks several rules, the first matching reason in this list is recorded.
    rules = [
        (df["vendor_id"].isna(), "MISSING_VENDOR_ID"),
        (df["vendor_name"].isna(), "MISSING_VENDOR_NAME"),
        (df["contract_value_usd"].isna(), "INVALID_CONTRACT_VALUE"),
        (df["lead_time_days"] < 0, "NEGATIVE_LEAD_TIME"),
        (df["lead_time_days"].isna() & df["lead_time_raw"].notna(), "INVALID_LEAD_TIME"),
        (df["country"].isna(), "MISSING_COUNTRY"),
        (df["onboarded_date"].isna(), "INVALID_ONBOARDED_DATE"),
        (df["last_modified"].isna(), "INVALID_LAST_MODIFIED"),
    ]
    conditions = [condition.fillna(False) for condition, _ in rules]
    reasons = [reason for _, reason in rules]

    df = df.copy()
    df["rejection_reason"] = np.select(conditions, reasons, default="")

    is_invalid = df["rejection_reason"] != ""
    df_quarantine = df[is_invalid].copy()
    # Helper columns are only needed for the quarantine audit trail
    df_clean = df[~is_invalid].drop(columns=["rejection_reason", "contract_value_raw", "lead_time_raw"]).copy()

    for _, row in df_quarantine.iterrows():
        log.warning(f"    QUARANTINED {row['vendor_id']} ({row['source_system']}): {row['rejection_reason']}")

    stats["quarantined"], stats["valid"] = len(df_quarantine), len(df_clean)
    return df_clean, df_quarantine


# --------------------------------------------------------------------------- #
# Step 6: Missing value imputation & deduplication
# --------------------------------------------------------------------------- #
def impute_and_deduplicate(df_clean: pd.DataFrame) -> pd.DataFrame:
    """Impute missing lead times with the median of valid records, then keep the latest record per vendor."""
    df = df_clean.copy()

    # Median is calculated on ALL valid records BEFORE deduplication (14 for the sample data)
    median_lead_time = int(round(df["lead_time_days"].median()))
    stats["median_lead_time"] = median_lead_time

    df["is_lead_time_imputed"] = df["lead_time_days"].isna()
    for _, row in df[df["is_lead_time_imputed"]].iterrows():
        log.info(f"    {row['vendor_id']} ({row['source_system']}): lead_time_days missing -> "
                 f"imputed median {median_lead_time}")
    df["lead_time_days"] = df["lead_time_days"].fillna(median_lead_time).astype(int)
    stats["imputed"] = int(df["is_lead_time_imputed"].sum())

    # Sort oldest -> newest, then keep the LAST (latest) record for each vendor_id.
    # mergesort is a stable sort, so equal timestamps keep their original order.
    df = df.sort_values("last_modified", ascending=True, kind="mergesort")
    is_duplicate = df.duplicated(subset="vendor_id", keep="last")
    for _, row in df[is_duplicate].iterrows():
        log.info(f"    {row['vendor_id']}: older record from {row['source_system']} "
                 f"({row['last_modified']}) dropped, newer version kept")
    df = df.drop_duplicates(subset="vendor_id", keep="last").sort_values("vendor_id")

    stats["duplicates_removed"] = int(is_duplicate.sum())
    return df.reset_index(drop=True)


# --------------------------------------------------------------------------- #
# Step 7: Category normalization (.explode)
# --------------------------------------------------------------------------- #
def build_normalized_category_bridge(df_clean: pd.DataFrame) -> pd.DataFrame:
    """Split comma-separated material categories and explode them into vendor_id -> category pairs."""
    bridge = df_clean[["vendor_id", "material_categories"]].copy()

    # 'API, Excipient' -> ['API', 'Excipient']
    bridge["material_category"] = bridge["material_categories"].fillna("").str.split(",")
    bridge["material_category"] = bridge["material_category"].apply(
        lambda items: [item.strip() for item in items]
    )
    # One row per list item
    bridge = bridge.explode("material_category")

    # Remove empty categories and repeated pairs (the table's primary key is vendor_id + category)
    bridge = bridge[bridge["material_category"].notna() & (bridge["material_category"] != "")]
    bridge = bridge[["vendor_id", "material_category"]].drop_duplicates().reset_index(drop=True)
    return bridge


# --------------------------------------------------------------------------- #
# Steps 8-10: Database
# --------------------------------------------------------------------------- #
def connect(db_path):
    """Open the SQLite database file (created automatically) with foreign keys enforced."""
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def read_sql_statements(file_path):
    """Read a .sql file, drop -- comment lines and split it into statements."""
    with open(file_path, "r", encoding="utf-8") as file:
        lines = [line for line in file if not line.strip().startswith("--")]
    return [stmt.strip() for stmt in "".join(lines).split(";") if stmt.strip()]


def setup_database(db_path, ddl_file):
    """Create the database file and run the DDL to create the three tables."""
    conn = None
    try:
        conn = connect(db_path)
        log.info(f"    Database file ready: {db_path}")
        for statement in read_sql_statements(ddl_file):
            conn.execute(statement)
            log.info(f"    Executed: {' '.join(statement.split()[:6])} ...")
        conn.commit()
    finally:
        if conn is not None:
            conn.close()


def to_sql_rows(df, columns):
    """DataFrame -> list of tuples with plain Python values (NaN/NaT -> NULL)."""
    clean = df[columns].astype(object).where(pd.notna(df[columns]), None)
    return [tuple(row) for row in clean.itertuples(index=False)]


def persist_to_database(df_clean: pd.DataFrame, df_bridge: pd.DataFrame,
                        df_quarantine: pd.DataFrame, db_path: str):
    """
    Write the three frames into the target tables in ONE transaction.
    Each run replaces the previous run's rows, so re-running never breaks the primary keys.
    """
    processed_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # gold_vendor_master: material_categories is dropped (it lives in the bridge table)
    master = df_clean.drop(columns=["material_categories"]).copy()
    master["onboarded_date"] = master["onboarded_date"].dt.strftime("%Y-%m-%d")
    master["last_modified"] = master["last_modified"].dt.strftime("%Y-%m-%d %H:%M:%S")
    master["is_lead_time_imputed"] = master["is_lead_time_imputed"].astype(int)   # True/False -> 1/0
    master["lead_time_days"] = master["lead_time_days"].astype(int)
    master["processed_at"] = processed_at

    # err_vendor_quarantine: keep the original contract text (e.g. 'TBD') for the audit trail
    quarantine = df_quarantine.copy()
    quarantine["contract_value_usd"] = quarantine["contract_value_raw"]
    quarantine["onboarded_date"] = quarantine["onboarded_date"].dt.strftime("%Y-%m-%d")
    quarantine["last_modified"] = quarantine["last_modified"].dt.strftime("%Y-%m-%d %H:%M:%S")
    quarantine["processed_at"] = processed_at

    inserts = [
        ("gold_vendor_master", MASTER_COLUMNS, master),
        ("gold_vendor_categories", ["vendor_id", "material_category"], df_bridge),
        ("err_vendor_quarantine", QUARANTINE_COLUMNS, quarantine),
    ]

    conn = None
    try:
        conn = connect(db_path)
        cursor = conn.cursor()

        # Clear the previous run (child table first because of the foreign key)
        for table in ("gold_vendor_categories", "gold_vendor_master", "err_vendor_quarantine"):
            cursor.execute(f"DELETE FROM {table}")

        for table, columns, frame in inserts:
            sql = (f"INSERT INTO {table} ({', '.join(columns)}) "
                   f"VALUES ({', '.join(['?'] * len(columns))})")
            rows = to_sql_rows(frame, columns)
            cursor.executemany(sql, rows)               # parameterized (?): no string formatting
            log.info(f"    {table}: {len(rows)} row(s) inserted")

        conn.commit()
        log.info("    COMMIT: all three tables saved together")
    except sqlite3.Error:
        if conn is not None:
            conn.rollback()
            log.warning("    ROLLBACK: no tables were changed")
        raise
    finally:
        if conn is not None:
            conn.close()
            log.info("    Database connection closed")


def export_quarantine_report(db_path, report_path):
    """Export the err_vendor_quarantine table contents to quarantine_report.csv."""
    conn = None
    try:
        conn = connect(db_path)
        report = pd.read_sql_query("SELECT * FROM err_vendor_quarantine ORDER BY vendor_id", conn)
    finally:
        if conn is not None:
            conn.close()
    os.makedirs(os.path.dirname(report_path), exist_ok=True)
    report.to_csv(report_path, index=False, encoding="utf-8")
    return len(report)


def run_verification_queries(db_path, verification_file):
    """Run every query in the verification file, log the results and collect table counts."""
    conn = None
    try:
        conn = connect(db_path)
        for number, statement in enumerate(read_sql_statements(verification_file), start=1):
            result = pd.read_sql_query(statement, conn)
            log.info(f"    Query {number}: {' '.join(statement.split()[:6])} ...")
            log_frame(result)
            if "total_portfolio_spend_usd" in result.columns:
                stats["total_spend"] = result.iloc[0, 0]

        for key, table in (("master_rows", "gold_vendor_master"),
                           ("category_rows", "gold_vendor_categories"),
                           ("quarantine_rows", "err_vendor_quarantine")):
            stats[key] = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    finally:
        if conn is not None:
            conn.close()


# --------------------------------------------------------------------------- #
# Step 11: Summary
# --------------------------------------------------------------------------- #
def print_summary(report_cfg, status):
    line = "=" * 60
    spend = f"{stats['total_spend']:,.2f}" if stats["total_spend"] is not None else "n/a"
    summary = [
        line,
        "VENDOR MASTER CONSOLIDATION SUMMARY",
        line,
        f"{'ERP Rows Read':<38}: {stats['erp_rows']}",
        f"{'MANUAL_LOG Rows Read':<38}: {stats['manual_rows']}",
        f"{'Total Input Rows':<38}: {stats['erp_rows'] + stats['manual_rows']}",
        "-" * 60,
        f"{'Records Quarantined':<38}: {stats['quarantined']}",
        f"{'Records Passed Quality Gate':<38}: {stats['valid']}",
        f"{'Lead Times Imputed (median ' + str(stats['median_lead_time']) + ')':<38}: {stats['imputed']}",
        f"{'Duplicate Records Removed':<38}: {stats['duplicates_removed']}",
        "-" * 60,
        f"{'gold_vendor_master Rows':<38}: {stats['master_rows']}",
        f"{'gold_vendor_categories Rows':<38}: {stats['category_rows']}",
        f"{'err_vendor_quarantine Rows':<38}: {stats['quarantine_rows']}",
        f"{'Total Portfolio Spend (USD)':<38}: {spend}",
        f"{'Quarantine Report':<38}: {report_cfg}",
        line,
        f"{'Status':<38}: {status}",
        line,
    ]
    for text in summary:
        log.info(text)


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def main():
    # STEP 1: Configuration and logging
    try:
        config = load_config()
        paths = config["paths"]
        erp_file = resolve_path(paths["erp_file"])
        manual_file = resolve_path(paths["manual_file"])
        db_file = resolve_path(paths["db_file"])
        ddl_file = resolve_path(paths["ddl_file"])
        verification_file = resolve_path(paths["verification_file"])
        report_cfg = paths["quarantine_report"]
        report_file = resolve_path(report_cfg)
        expected_master = int(config["validation"]["expected_master_rows"])
        expected_quarantine = int(config["validation"]["expected_quarantine_rows"])
        log_file = setup_logging(resolve_path(paths["log_dir"]))
    except (FileNotFoundError, KeyError, ValueError) as e:
        print(f"Configuration error: {e}")
        return 1

    log.info(f"{SCRIPT_NAME}.py started at {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    log.info("step_1 completed: configuration loaded from config.ini")
    status = "SUCCESS"

    try:
        # STEP 2: Ingestion & lineage tagging
        log.info("step_2: Ingestion & source lineage tagging started")
        raw_df = load_raw_sources(erp_file, manual_file)
        log.info(f"step_2 completed: {len(raw_df)} rows combined with pd.concat()")

        # STEP 3: String & country standardization
        log.info("step_3: String & country standardization started")
        std_df = standardize_strings_and_countries(raw_df)
        log_frame(std_df, ["vendor_id", "vendor_name", "country", "source_system"])
        log.info("step_3 completed successfully")

        # STEP 4: Numeric & date parsing
        log.info("step_4: Numeric & date parsing started")
        parsed_df = parse_types_and_dates(std_df)
        log.info("step_4 completed successfully")

        # STEP 5: Quarantine split
        log.info("step_5: Data quality gate (quarantine split) started")
        df_clean, df_quarantine = split_quarantine_records(parsed_df)
        log.info(f"step_5 completed: {len(df_clean)} valid, {len(df_quarantine)} quarantined")

        # STEP 6: Imputation & deduplication
        log.info("step_6: Imputation & deduplication started")
        df_clean_dedup = impute_and_deduplicate(df_clean)
        buffer = io.StringIO()
        df_clean_dedup.info(buf=buffer)
        for line in buffer.getvalue().splitlines():
            log.info(f"        {line}")
        log.info(f"step_6 completed: {len(df_clean_dedup)} unique vendors")

        # STEP 7: Category explosion
        log.info("step_7: Category normalization (.explode) started")
        df_bridge = build_normalized_category_bridge(df_clean_dedup)
        log_frame(df_bridge)
        log.info(f"step_7 completed: {len(df_bridge)} vendor-category pairs")

        # STEP 8: Database setup
        log.info("step_8: Database setup started")
        setup_database(db_file, ddl_file)
        log.info("step_8 completed: gold_vendor_master, gold_vendor_categories, err_vendor_quarantine ready")

        # STEP 9: Load
        log.info("step_9: Loading target tables started")
        persist_to_database(df_clean_dedup, df_bridge, df_quarantine, db_file)
        log.info("step_9 completed successfully")

        # STEP 10: Quarantine report + verification
        log.info("step_10: Quarantine report export and verification started")
        exported = export_quarantine_report(db_file, report_file)
        log.info(f"    {exported} row(s) exported to {report_cfg}")
        run_verification_queries(db_file, verification_file)
        log.info("step_10 completed successfully")

        if stats["master_rows"] != expected_master or stats["quarantine_rows"] != expected_quarantine:
            status = "COMPLETED WITH WARNINGS"
            log.warning(f"Expected {expected_master} master / {expected_quarantine} quarantine rows; "
                        f"got {stats['master_rows']} / {stats['quarantine_rows']}")

    except sqlite3.Error as e:
        status = "FAILED"
        log.error(f"Database error: {e}")
    except (OSError, ValueError, KeyError) as e:
        status = "FAILED"
        log.error(f"Pipeline error: {e}")
    except Exception as e:
        status = "FAILED"
        log.exception(f"Unexpected error: {e}")

    # STEP 11: Summary
    print_summary(report_cfg, status)
    log.info(f"{SCRIPT_NAME}.py {'failed' if status == 'FAILED' else 'completed'} "
             f"at {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    log.info(f"Log file: {log_file}")
    return 0 if status == "SUCCESS" else 1


if __name__ == "__main__":
    sys.exit(main())


"""
Log File Name: <Scriptname_Timestamp.log>
Example      : logs/consolidate_vendor_master_20261125_140000.log

"""
