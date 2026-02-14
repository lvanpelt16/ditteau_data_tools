#!/usr/bin/python3
import pyarrow.csv as pv
import pyarrow.parquet as pq
import os
import tempfile

export_dir = "/extra/LASTBACK/UNL"
parquet_dir = "/extra/LASTBACK/PARQ"

def fix_unl(filepath):
    """Fix Informix escape conventions"""
    with open(filepath, 'r', encoding='latin-1') as f:
        content = f.read()
    content = content.replace('\\\\', '\x00')   # escaped backslashes first
    content = content.replace('\\|', '¦')        # escaped pipe to safe char
    content = content.replace('\\\n', ' ')       # rejoin continuation lines
    content = content.replace('\x00', '\\')      # restore literal backslashes
    return content

for f in os.listdir(export_dir):
    if f.endswith(".unl"):
        try:
            fixed_content = fix_unl(os.path.join(export_dir, f))
            tmp = os.path.join(export_dir, f + ".tmp")
            with open(tmp, 'w', encoding='latin-1') as out:
                out.write(fixed_content)
            table = pv.read_csv(
                tmp,
                parse_options=pv.ParseOptions(delimiter="|"),
                read_options=pv.ReadOptions(encoding="latin-1")
            )
            pq.write_table(
                table,
                os.path.join(parquet_dir, f.replace(".unl", ".parquet")),
                compression="snappy"
            )
            os.remove(tmp)
            print("Converted " + f)
        except Exception as e:
            print("FAILED " + f + ": " + str(e))
