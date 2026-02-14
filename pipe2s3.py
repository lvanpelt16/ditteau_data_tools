#!/usr/bin/env python3
import os
import sys
import subprocess
import pyarrow.csv as pv
import pyarrow.parquet as pq

DATABASE = "cars"
UNL_DIR = "/extra/LASTBACK/UNL"
PARQ_DIR = "/extra/LASTBACK/PARQ"
S3_BUCKET = "s3://merrimack-last-backup"

def get_tables():
    """Get list of user tables from systables"""
    sql = "SELECT TRIM(tabname) FROM systables WHERE tabtype = 'T' AND tabid >= 100 and nrows > 0;"
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

def unload_table(table):
    """Unload a single table to .unl file"""
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

def fix_unl(filepath):
    """Fix Informix escape conventions"""
    with open(filepath, 'r', encoding='latin-1') as f:
        content = f.read()
    content = content.replace('\\\\', '\x00')
    content = content.replace('\\|', '\xA6')
    content = content.replace('\\\n', ' ')
    content = content.replace('\x00', '\\')
    return content

def convert_to_parquet(table, unl_path):
    """Convert .unl to parquet"""
    parq_path = os.path.join(PARQ_DIR, table + ".parquet")
    tmp_path = unl_path + ".tmp"

    fixed_content = fix_unl(unl_path)
    with open(tmp_path, 'w', encoding='latin-1') as out:
        out.write(fixed_content)

    arrow_table = pv.read_csv(
        tmp_path,
        parse_options=pv.ParseOptions(delimiter="|"),
        read_options=pv.ReadOptions(encoding="latin-1")
    )
    pq.write_table(arrow_table, parq_path, compression="snappy")
    os.remove(tmp_path)
    return parq_path

def s3_copy(local_path, s3_dir):
    """Copy file to S3"""
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

def main():
    dry_run = "--dry-run" in sys.argv

    os.makedirs(UNL_DIR, exist_ok=True)
    os.makedirs(PARQ_DIR, exist_ok=True)

    tables = get_tables()
    print("Found {} tables".format(len(tables)))

    if dry_run:
        print("\n=== DRY RUN - No changes will be made ===\n")
        for i, table in enumerate(tables):
            print("[{}/{}] {}".format(i + 1, len(tables), table))
        print("\nS3 destinations:")
        print("  Raw:     {}/raw-unload/".format(S3_BUCKET))
        print("  Parquet: {}/parquet-files/".format(S3_BUCKET))
        print("\nRun without --dry-run to execute.")
        return

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
            print("  Unloaded: " + unl_path)

            # Step 2: Copy .unl to S3
            print("  Uploading .unl to S3...")
            s3_copy(unl_path, "raw-unload")

            # Step 3: Convert to Parquet
            print("  Converting to Parquet...")
            parq_path = convert_to_parquet(table, unl_path)
            print("  Converted: " + parq_path)

            # Step 4: Copy Parquet to S3
            print("  Uploading Parquet to S3...")
            s3_copy(parq_path, "parquet-files")

            # Step 5: Clean up local files
            os.remove(unl_path)
            os.remove(parq_path)
            print("  DONE - local files removed")
            succeeded.append(table)

        except Exception as e:
            print("  FAILED: " + str(e))
            failed.append(table)
            # Clean up whatever exists
            for path in [unl_path, parq_path]:
                if path and os.path.exists(path):
                    os.remove(path)

    print("\n" + "=" * 50)
    print("COMPLETE: {} succeeded, {} failed".format(len(succeeded), len(failed)))
    if failed:
        print("Failed tables:")
        for t in failed:
            print("  " + t)

if __name__ == "__main__":
    main()
