#!/usr/bin/env python3
"""
pipe2s3.py — Unload Informix tables, convert to Parquet, upload to S3
=====================================================================
Processes one table at a time to conserve disk space:
  1. Unload table to .unl file
  2. Upload raw .unl to S3
  3. Convert .unl to Parquet (with proper column names)
  4. Upload Parquet to S3
  5. Delete local files
  6. Move to next table

Usage:
    ./pipe2s3.py              # Run full pipeline
    ./pipe2s3.py --dry-run    # List tables only, no changes
"""

import os
import sys
import subprocess
import pyarrow.csv as pv
import pyarrow.parquet as pq

# ─────────────────────────────────────────────
# Configuration — update these for your environment
# ─────────────────────────────────────────────

DATABASE = "cars"
UNL_DIR = "/extra/LASTBACK/UNL"
PARQ_DIR = "/extra/LASTBACK/PARQ"
S3_BUCKET = "s3://merrimack-last-backup"


# ─────────────────────────────────────────────
# Database functions
# ─────────────────────────────────────────────

def get_tables():
    """Get list of user tables from systables."""
    sql = "SELECT TRIM(tabname) FROM systables WHERE tabtype = 'T' AND tabid >= 100 AND nrows > 0;"
    result = subprocess.run(
        ["dbaccess", DATABASE, "-"],
        input=sql,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True
    )
    tables = []
    for line in result.stdout.splitlines():
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
    result = subprocess.run(
        ["dbaccess", DATABASE, "-"],
        input=sql,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True
    )
    columns = []
    for line in result.stdout.splitlines():
        line = line.strip()
        if line.startswith("(expression)"):
            col = line.replace("(expression)", "").strip()
            if col:
                columns.append(col)
    return columns


def unload_table(table):
    """Unload a single table to .unl file."""
    unl_path = os.path.join(UNL_DIR, table + ".unl")
    sql = 'UNLOAD TO "{}" DELIMITER "|" SELECT * FROM {};'.format(unl_path, table)
    result = subprocess.run(
        ["dbaccess", DATABASE, "-"],
        input=sql,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True
    )
    if result.returncode != 0:
        raise Exception("Unload failed: " + result.stderr)
    return unl_path


# ─────────────────────────────────────────────
# File processing
# ─────────────────────────────────────────────

def fix_unl(filepath, expected_cols):
    """
    Fix Informix .unl escape conventions and reassemble broken rows.

    Handles:
      - Escaped backslashes (\\)
      - Escaped pipes (\|) — literal pipe in data
      - Continuation lines (\<newline>)
      - Trailing pipe per line (Informix convention)
      - HTML/multiline content causing fragmented rows
      - Short rows with omitted trailing empty fields
    """
    with open(filepath, 'r', encoding='latin-1') as f:
        content = f.read()

    # Step 1: Handle escaped backslashes — protect them first
    content = content.replace('\\\\', '\x00')

    # Step 2: Handle escaped pipes (literal pipe in data)
    content = content.replace('\\|', '\xA6')

    # Step 3: Handle continuation lines (escaped newlines)
    content = content.replace('\\\n', ' ')

    # Step 4: Restore literal backslashes
    content = content.replace('\x00', '\\')

    # Step 5: Strip trailing pipe per line (Informix adds one)
    lines = content.split('\n')
    cleaned = []
    for line in lines:
        if line.endswith('|'):
            line = line[:-1]
        cleaned.append(line)

    # Step 6: Reassemble rows that are still fragmented
    # Uses expected column count to detect and rejoin broken rows
    # (HTML content with unescaped newlines that slipped through)
    reassembled = []
    current = ""

    for line in cleaned:
        if not line:
            continue

        if current:
            candidate = current + " " + line
        else:
            candidate = line

        col_count = candidate.count('|') + 1

        if col_count == expected_cols:
            # Perfect — this is a complete row
            reassembled.append(candidate)
            current = ""
        elif col_count < expected_cols:
            # Row is incomplete — keep accumulating
            current = candidate
        else:
            # Too many columns — flush current, start fresh
            if current:
                reassembled.append(current)
            current = line
            if line.count('|') + 1 == expected_cols:
                reassembled.append(line)
                current = ""

    # Don't lose the last row
    if current:
        reassembled.append(current)

    # Step 7: Pad short rows with empty fields
    final = []
    for row in reassembled:
        col_count = row.count('|') + 1
        if col_count < expected_cols:
            row = row + '|' * (expected_cols - col_count)
        final.append(row)

    return '\n'.join(final) + '\n'


def convert_to_parquet(table, unl_path):
    """Convert .unl to Parquet with proper column names from syscolumns."""
    parq_path = os.path.join(PARQ_DIR, table + ".parquet")
    tmp_path = unl_path + ".tmp"

    # Get column names and expected count
    columns = get_columns(table)
    expected_cols = len(columns)

    if expected_cols == 0:
        raise Exception("Could not retrieve column names from syscolumns")

    # Fix Informix escape conventions and reassemble rows
    fixed_content = fix_unl(unl_path, expected_cols)
    with open(tmp_path, 'w', encoding='latin-1') as out:
        out.write(fixed_content)

    # Read with explicit column names and large block size
    read_opts = pv.ReadOptions(
        column_names=columns,
        autogenerate_column_names=False,
        encoding="latin-1",
        block_size=1073741824  # 1GB — handles large records
    )

    arrow_table = pv.read_csv(
        tmp_path,
        parse_options=pv.ParseOptions(delimiter="|"),
        read_options=read_opts,
    )

    pq.write_table(arrow_table, parq_path, compression="snappy")
    os.remove(tmp_path)
    return parq_path


# ─────────────────────────────────────────────
# S3 upload
# ─────────────────────────────────────────────

def s3_copy(local_path, s3_dir):
    """Copy file to S3."""
    filename = os.path.basename(local_path)
    s3_path = "{}/{}/{}".format(S3_BUCKET, s3_dir, filename)
    result = subprocess.run(
        ["aws", "s3", "cp", local_path, s3_path],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True
    )
    if result.returncode != 0:
        raise Exception("S3 upload failed: " + result.stderr)


# ─────────────────────────────────────────────
# Schema export
# ─────────────────────────────────────────────

def export_schema():
    """Export full database schema as a safety net."""
    schema_path = os.path.join(UNL_DIR, DATABASE + "_schema.sql")
    result = subprocess.run(
        ["dbschema", "-d", DATABASE],
        stdout=open(schema_path, 'w'),
        stderr=subprocess.PIPE,
        universal_newlines=True
    )
    if result.returncode == 0 and os.path.exists(schema_path):
        s3_copy(schema_path, "raw-unload")
        print("Schema exported and uploaded: " + schema_path)
    else:
        print("Warning: dbschema export failed — continuing anyway")


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

def main():
    dry_run = "--dry-run" in sys.argv

    os.makedirs(UNL_DIR, exist_ok=True)
    os.makedirs(PARQ_DIR, exist_ok=True)

    tables = get_tables()
    print("Found {} tables".format(len(tables)))

    if dry_run:
        print("\n=== DRY RUN - No changes will be made ===\n")
        for i, table in enumerate(tables):
            cols = get_columns(table)
            print("[{}/{}] {} ({} columns)".format(i + 1, len(tables), table, len(cols)))
        print("\nS3 destinations:")
        print("  Raw:     {}/raw-unload/".format(S3_BUCKET))
        print("  Parquet: {}/parquet-files/".format(S3_BUCKET))
        print("\nRun without --dry-run to execute.")
        return

    # Export schema first
    print("Exporting database schema...")
    export_schema()

    succeeded = []
    failed = []

    for i, table in enumerate(tables):
        print("\n[{}/{}] Processing: {}".format(i + 1, len(tables), table))
        unl_path = None
        parq_path = None
        try:
            # Step 1: Unload
            print("  Unloading...")
            unl_path = unload_table(table)
            size_mb = os.path.getsize(unl_path) / (1024 * 1024)
            print("  Unloaded: {} ({:.1f} MB)".format(unl_path, size_mb))

            # Step 2: Copy .unl to S3
            print("  Uploading .unl to S3...")
            s3_copy(unl_path, "raw-unload")

            # Step 3: Convert to Parquet
            print("  Converting to Parquet...")
            parq_path = convert_to_parquet(table, unl_path)
            parq_mb = os.path.getsize(parq_path) / (1024 * 1024)
            print("  Converted: {} ({:.1f} MB)".format(parq_path, parq_mb))

            # Step 4: Copy Parquet to S3
            print("  Uploading Parquet to S3...")
            s3_copy(parq_path, "parquet-files")

            # Step 5: Clean up local files
            os.remove(unl_path)
            os.remove(parq_path)
            print("  DONE - local files removed (saved {:.1f} MB local)".format(size_mb + parq_mb))
            succeeded.append(table)

        except Exception as e:
            print("  FAILED: " + str(e))
            failed.append({"table": table, "error": str(e)})
            # Clean up whatever exists
            for path in [unl_path, parq_path]:
                if path and os.path.exists(path):
                    os.remove(path)
            # Also clean up any .tmp file
            if unl_path and os.path.exists(unl_path + ".tmp"):
                os.remove(unl_path + ".tmp")

    # ── Summary ──────────────────────────────

    print("\n" + "=" * 60)
    print("COMPLETE: {} succeeded, {} failed out of {} tables".format(
        len(succeeded), len(failed), len(tables)))
    print("=" * 60)

    if failed:
        print("\nFailed tables:")
        print("-" * 60)
        for f in failed:
            print("  {} — {}".format(f["table"], f["error"]))

    # Write failure log
    if failed:
        log_path = os.path.join(UNL_DIR, "failed_tables.log")
        with open(log_path, 'w') as log:
            for f in failed:
                log.write("{}\t{}\n".format(f["table"], f["error"]))
        print("\nFailure log written to: " + log_path)


if __name__ == "__main__":
    main()
