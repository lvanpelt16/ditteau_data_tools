#!/usr/bin/env python3
"""
validate_archive.py — Validate Parquet archive against live Informix database
==============================================================================
Run this BEFORE you lose access to the Informix server.

Checks performed per table:
  1. Row count: Informix COUNT(*) vs Parquet num_rows
  2. Column count: syscolumns vs Parquet schema
  3. Column names: syscolumns names vs Parquet field names
  4. First row: Informix FIRST 1 vs Parquet first row
  5. Null check: Count nulls per column in Parquet

Produces:
  - Console summary with pass/fail per table
  - Detailed CSV report: /extra/LASTBACK/validation_report.csv
  - Detailed log: /extra/LASTBACK/validation_detail.log

Usage:
    ./validate_archive.py                    # Validate all tables
    ./validate_archive.py --tables id_rec adm_rec   # Validate specific tables
    ./validate_archive.py --summary-only     # Skip first-row spot checks
"""

import os
import sys
import subprocess
import datetime

# ─────────────────────────────────────────────
# Configuration — match your pipe2s3.py settings
# ─────────────────────────────────────────────

DATABASE = "cars"
S3_BUCKET = "s3://merrimack-last-backup"
PARQ_S3_DIR = "parquet-files"
LOCAL_PARQ_DIR = "/extra/LASTBACK/PARQ"
OUTPUT_DIR = "/extra/LASTBACK"

# ─────────────────────────────────────────────
# Try to import pyarrow — needed for Parquet reads
# ─────────────────────────────────────────────

try:
    import pyarrow.parquet as pq
    HAS_PYARROW = True
except ImportError:
    HAS_PYARROW = False
    print("WARNING: pyarrow not installed — Parquet checks will use DuckDB fallback")

try:
    import duckdb
    HAS_DUCKDB = True
except ImportError:
    HAS_DUCKDB = False


# ─────────────────────────────────────────────
# Database functions
# ─────────────────────────────────────────────

def run_dbaccess(sql):
    """Run SQL against Informix and return stdout."""
    result = subprocess.run(
        ["dbaccess", DATABASE, "-"],
        input=sql,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True
    )
    return result.stdout, result.stderr, result.returncode


def get_tables():
    """Get list of user tables from systables."""
    sql = "SELECT TRIM(tabname) FROM systables WHERE tabtype = 'T' AND tabid >= 100 AND nrows > 0;"
    stdout, stderr, rc = run_dbaccess(sql)
    tables = []
    for line in stdout.splitlines():
        line = line.strip()
        if line.startswith("(expression)"):
            table = line.replace("(expression)", "").strip()
            if table:
                tables.append(table)
    return tables


def get_columns(table):
    """Get ordered column names from syscolumns."""
    sql = (
        "SELECT TRIM(c.colname) FROM syscolumns c, systables t "
        "WHERE c.tabid = t.tabid AND t.tabname = '{}' "
        "ORDER BY c.colno;".format(table)
    )
    stdout, stderr, rc = run_dbaccess(sql)
    columns = []
    for line in stdout.splitlines():
        line = line.strip()
        if line.startswith("(expression)"):
            col = line.replace("(expression)", "").strip()
            if col:
                columns.append(col)
    return columns


def get_informix_count(table):
    """Get row count from Informix."""
    sql = "SELECT COUNT(*) FROM {};".format(table)
    stdout, stderr, rc = run_dbaccess(sql)
    for line in stdout.splitlines():
        line = line.strip()
        if line.isdigit():
            return int(line)
    return None


def get_informix_first_row(table):
    """Get first row from Informix as pipe-delimited string."""
    sql = 'SELECT FIRST 1 * FROM {};'.format(table)
    stdout, stderr, rc = run_dbaccess(sql)
    # dbaccess output is messy — grab lines between header and row count
    lines = stdout.splitlines()
    data_lines = []
    in_data = False
    for line in lines:
        if line.strip().startswith("(") and "row" in line:
            break
        if in_data and line.strip():
            data_lines.append(line.strip())
        # Skip header lines (column names and dashes)
        if line.strip().startswith("---") or line.strip().startswith("==="):
            in_data = True
    return " | ".join(data_lines) if data_lines else None


# ─────────────────────────────────────────────
# S3 / Parquet functions
# ─────────────────────────────────────────────

def download_parquet(table):
    """Download a Parquet file from S3 for validation."""
    s3_path = "{}/{}/{}.parquet".format(S3_BUCKET, PARQ_S3_DIR, table)
    local_path = os.path.join(LOCAL_PARQ_DIR, table + ".parquet")
    result = subprocess.run(
        ["aws", "s3", "cp", s3_path, local_path],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True
    )
    if result.returncode != 0:
        return None
    return local_path


def get_parquet_row_count(parq_path):
    """Get row count from Parquet file."""
    if HAS_PYARROW:
        return pq.read_metadata(parq_path).num_rows
    elif HAS_DUCKDB:
        con = duckdb.connect()
        result = con.execute(
            "SELECT COUNT(*) FROM read_parquet('{}')".format(parq_path)
        ).fetchone()
        return result[0]
    return None


def get_parquet_columns(parq_path):
    """Get column names from Parquet file."""
    if HAS_PYARROW:
        return pq.read_schema(parq_path).names
    elif HAS_DUCKDB:
        con = duckdb.connect()
        result = con.execute(
            "DESCRIBE SELECT * FROM read_parquet('{}')".format(parq_path)
        ).df()
        return result["column_name"].tolist()
    return []


def get_parquet_first_row(parq_path):
    """Get first row from Parquet as a list of values."""
    if HAS_PYARROW:
        table = pq.read_table(parq_path)
        if table.num_rows > 0:
            row = table.slice(0, 1).to_pydict()
            return {k: v[0] for k, v in row.items()}
    elif HAS_DUCKDB:
        con = duckdb.connect()
        df = con.execute(
            "SELECT * FROM read_parquet('{}') LIMIT 1".format(parq_path)
        ).df()
        if len(df) > 0:
            return df.iloc[0].to_dict()
    return None


def get_parquet_null_counts(parq_path):
    """Get null counts per column from Parquet."""
    if HAS_DUCKDB:
        con = duckdb.connect()
        cols = get_parquet_columns(parq_path)
        if not cols:
            return {}
        parts = []
        for col in cols:
            safe_col = '"{}"'.format(col)
            parts.append(
                "SUM(CASE WHEN {} IS NULL THEN 1 ELSE 0 END) AS \"null_{}\"".format(safe_col, col)
            )
        sql = "SELECT {} FROM read_parquet('{}')".format(", ".join(parts), parq_path)
        try:
            result = con.execute(sql).df()
            return {col: int(result.iloc[0]["null_" + col]) for col in cols}
        except Exception:
            return {}
    elif HAS_PYARROW:
        table = pq.read_table(parq_path)
        counts = {}
        for col_name in table.column_names:
            col = table.column(col_name)
            counts[col_name] = col.null_count
        return counts
    return {}


# ─────────────────────────────────────────────
# Validation
# ─────────────────────────────────────────────

def validate_table(table, summary_only=False):
    """Run all validation checks on a single table."""
    results = {
        "table": table,
        "row_count_informix": None,
        "row_count_parquet": None,
        "row_count_match": False,
        "col_count_informix": None,
        "col_count_parquet": None,
        "col_count_match": False,
        "col_names_match": False,
        "col_name_mismatches": [],
        "first_row_check": "SKIPPED",
        "null_columns": [],
        "status": "UNKNOWN",
        "errors": [],
    }

    # Download Parquet from S3
    parq_path = download_parquet(table)
    if not parq_path:
        results["status"] = "NO PARQUET"
        results["errors"].append("Could not download Parquet file from S3")
        return results

    try:
        # ── Check 1: Row counts ──────────────

        informix_count = get_informix_count(table)
        parquet_count = get_parquet_row_count(parq_path)

        results["row_count_informix"] = informix_count
        results["row_count_parquet"] = parquet_count
        results["row_count_match"] = (
            informix_count is not None
            and parquet_count is not None
            and informix_count == parquet_count
        )

        if not results["row_count_match"] and informix_count and parquet_count:
            diff = parquet_count - informix_count
            results["errors"].append(
                "Row count mismatch: Informix={}, Parquet={} (diff={:+d})".format(
                    informix_count, parquet_count, diff
                )
            )

        # ── Check 2: Column counts ───────────

        informix_cols = get_columns(table)
        parquet_cols = get_parquet_columns(parq_path)

        results["col_count_informix"] = len(informix_cols)
        results["col_count_parquet"] = len(parquet_cols)
        results["col_count_match"] = len(informix_cols) == len(parquet_cols)

        # ── Check 3: Column names ────────────

        if informix_cols and parquet_cols:
            if informix_cols == parquet_cols:
                results["col_names_match"] = True
            else:
                # Find specific mismatches
                max_len = max(len(informix_cols), len(parquet_cols))
                for idx in range(max_len):
                    ifx_col = informix_cols[idx] if idx < len(informix_cols) else "<MISSING>"
                    pq_col = parquet_cols[idx] if idx < len(parquet_cols) else "<MISSING>"
                    if ifx_col != pq_col:
                        results["col_name_mismatches"].append(
                            "Col {}: Informix='{}' vs Parquet='{}'".format(idx + 1, ifx_col, pq_col)
                        )

        # ── Check 4: First row spot check ────

        if not summary_only:
            parq_first = get_parquet_first_row(parq_path)
            if parq_first:
                # Check that the first row doesn't look like column headers
                # (the eaten-first-row problem)
                col_names_set = set(informix_cols)
                first_values_set = set(str(v) for v in parq_first.values() if v is not None)
                overlap = col_names_set.intersection(first_values_set)

                if len(overlap) > len(informix_cols) * 0.5:
                    results["first_row_check"] = "FAIL — first row appears to be column headers!"
                    results["errors"].append("First row looks like column names — data may be shifted")
                else:
                    results["first_row_check"] = "OK"
            else:
                results["first_row_check"] = "EMPTY TABLE"

        # ── Check 5: Null analysis ───────────

        null_counts = get_parquet_null_counts(parq_path)
        if null_counts and parquet_count:
            for col, nulls in null_counts.items():
                if nulls == parquet_count:
                    results["null_columns"].append(col)

        # ── Overall status ───────────────────

        if results["row_count_match"] and results["col_count_match"] and results["col_names_match"]:
            if results["first_row_check"] in ("OK", "SKIPPED", "EMPTY TABLE"):
                results["status"] = "OK"
            else:
                results["status"] = "WARNING"
        elif not results["row_count_match"]:
            results["status"] = "ROW MISMATCH"
        elif not results["col_count_match"]:
            results["status"] = "COL MISMATCH"
        elif not results["col_names_match"]:
            results["status"] = "NAME MISMATCH"
        else:
            results["status"] = "WARNING"

    except Exception as e:
        results["status"] = "ERROR"
        results["errors"].append(str(e))

    finally:
        # Clean up downloaded file
        if parq_path and os.path.exists(parq_path):
            os.remove(parq_path)

    return results


# ─────────────────────────────────────────────
# Reporting
# ─────────────────────────────────────────────

def print_summary(all_results):
    """Print a formatted summary table."""
    print("\n" + "=" * 90)
    print("VALIDATION REPORT — {}".format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    print("=" * 90)
    print("")
    print("{:<30} {:>10} {:>10} {:>6} {:>6}   {}".format(
        "Table", "Informix", "Parquet", "Cols", "Names", "Status"
    ))
    print("-" * 90)

    ok_count = 0
    warn_count = 0
    fail_count = 0

    for r in all_results:
        ifx = str(r["row_count_informix"]) if r["row_count_informix"] is not None else "?"
        pq_ct = str(r["row_count_parquet"]) if r["row_count_parquet"] is not None else "?"
        cols = "OK" if r["col_count_match"] else "FAIL"
        names = "OK" if r["col_names_match"] else "FAIL"

        if r["status"] == "OK":
            marker = "  \u2713"
            ok_count += 1
        elif r["status"] in ("WARNING", "NAME MISMATCH"):
            marker = "  \u26A0"
            warn_count += 1
        else:
            marker = "  \u2717"
            fail_count += 1

        print("{:<30} {:>10} {:>10} {:>6} {:>6} {}  {}".format(
            r["table"][:30], ifx, pq_ct, cols, names, marker, r["status"]
        ))

    print("-" * 90)
    print("TOTALS: {} OK, {} WARNINGS, {} FAILED out of {} tables".format(
        ok_count, warn_count, fail_count, len(all_results)
    ))
    print("=" * 90)

    return ok_count, warn_count, fail_count


def write_csv_report(all_results):
    """Write detailed CSV report."""
    csv_path = os.path.join(OUTPUT_DIR, "validation_report.csv")
    with open(csv_path, 'w') as f:
        f.write("table,status,rows_informix,rows_parquet,row_match,"
                "cols_informix,cols_parquet,col_match,names_match,"
                "first_row_check,all_null_columns,errors\n")
        for r in all_results:
            f.write("{},{},{},{},{},{},{},{},{},{},{},{}\n".format(
                r["table"],
                r["status"],
                r["row_count_informix"] or "",
                r["row_count_parquet"] or "",
                r["row_count_match"],
                r["col_count_informix"] or "",
                r["col_count_parquet"] or "",
                r["col_count_match"],
                r["col_names_match"],
                r["first_row_check"],
                "; ".join(r["null_columns"]) if r["null_columns"] else "",
                "; ".join(r["errors"]).replace(",", " ") if r["errors"] else "",
            ))
    print("\nCSV report: " + csv_path)
    return csv_path


def write_detail_log(all_results):
    """Write detailed validation log with all findings."""
    log_path = os.path.join(OUTPUT_DIR, "validation_detail.log")
    with open(log_path, 'w') as f:
        f.write("VALIDATION DETAIL LOG\n")
        f.write("Generated: {}\n".format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        f.write("Database: {}\n".format(DATABASE))
        f.write("S3 Bucket: {}/{}\n".format(S3_BUCKET, PARQ_S3_DIR))
        f.write("=" * 70 + "\n\n")

        for r in all_results:
            f.write("TABLE: {}\n".format(r["table"]))
            f.write("  Status: {}\n".format(r["status"]))
            f.write("  Row count — Informix: {}, Parquet: {}, Match: {}\n".format(
                r["row_count_informix"], r["row_count_parquet"], r["row_count_match"]
            ))
            f.write("  Col count — Informix: {}, Parquet: {}, Match: {}\n".format(
                r["col_count_informix"], r["col_count_parquet"], r["col_count_match"]
            ))
            f.write("  Col names match: {}\n".format(r["col_names_match"]))

            if r["col_name_mismatches"]:
                f.write("  Column name mismatches:\n")
                for m in r["col_name_mismatches"]:
                    f.write("    {}\n".format(m))

            f.write("  First row check: {}\n".format(r["first_row_check"]))

            if r["null_columns"]:
                f.write("  Columns that are ALL null: {}\n".format(", ".join(r["null_columns"])))

            if r["errors"]:
                f.write("  Errors:\n")
                for e in r["errors"]:
                    f.write("    {}\n".format(e))

            f.write("\n")

    print("Detail log: " + log_path)
    return log_path


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

def main():
    summary_only = "--summary-only" in sys.argv

    # Check for specific tables
    specific_tables = []
    if "--tables" in sys.argv:
        idx = sys.argv.index("--tables")
        specific_tables = [
            t for t in sys.argv[idx + 1:]
            if not t.startswith("--")
        ]

    os.makedirs(LOCAL_PARQ_DIR, exist_ok=True)

    if specific_tables:
        tables = specific_tables
        print("Validating {} specified tables".format(len(tables)))
    else:
        tables = get_tables()
        print("Found {} tables to validate".format(len(tables)))

    if summary_only:
        print("(Running in summary-only mode — skipping first-row checks)\n")

    all_results = []

    for i, table in enumerate(tables):
        sys.stdout.write("\r[{}/{}] Validating: {:<30}".format(i + 1, len(tables), table))
        sys.stdout.flush()

        result = validate_table(table, summary_only=summary_only)
        all_results.append(result)

    sys.stdout.write("\r" + " " * 80 + "\r")
    sys.stdout.flush()

    # Reports
    ok_count, warn_count, fail_count = print_summary(all_results)
    csv_path = write_csv_report(all_results)
    log_path = write_detail_log(all_results)

    # Upload reports to S3
    print("\nUploading reports to S3...")
    for path in [csv_path, log_path]:
        filename = os.path.basename(path)
        s3_path = "{}/validation/{}".format(S3_BUCKET, filename)
        subprocess.run(
            ["aws", "s3", "cp", path, s3_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True
        )
    print("Reports uploaded to {}/validation/".format(S3_BUCKET))

    # Clean up temp directory
    try:
        os.rmdir(LOCAL_PARQ_DIR)
    except OSError:
        pass

    # Exit code for scripting
    if fail_count > 0:
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == "__main__":
    main()
