"""
For context: the time and the file size pyarrow has for writing the same data as bench/write.php (read from the same files, then written with
the same codec and pyarrow's defaults: dictionary on, statistics on, data page v1, row groups of up to 1M rows, page index off unless --index).

    research/venv/bin/python bench/pyarrow-write.py [runs] [--index]

A different runtime doing different work (Arrow arrays in, native encoding); the table is for scale, not a like-for-like comparison.
"""
import io
import os
import sys
import time

import pyarrow.parquet as pq

DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
args = [a for a in sys.argv[1:] if not a.startswith("--")]
RUNS = int(args[0]) if args else 3
INDEX = "--index" in sys.argv
names = ["int64-plain", "int64-dict", "double", "str-lowcard", "str-highcard", "opt-10pct-null", "decimal", "timestamp", "list-int32-tokens", "struct-3", "wide-50"]
print("pyarrow %s; sizes in bytes, ms best of %d" % (__import__("pyarrow").__version__, RUNS))
print("%-22s %-13s %11s %9s" % ("dataset", "codec", "out bytes", "ms"))
for n in names:
    table = pq.read_table(os.path.join(DIR, n + ".snappy.parquet"))
    for codec in ["none", "snappy", "gzip", "zstd"]:
        def write():
            sink = io.BytesIO()
            pq.write_table(table, sink, compression=codec, write_page_index=INDEX, row_group_size=1 << 20)
            return sink.tell()
        size = write()
        best = float("inf")
        for _ in range(RUNS):
            t = time.perf_counter()
            write()
            best = min(best, time.perf_counter() - t)
        print("%-22s %-13s %11d %9.0f" % (n, codec if codec != "none" else "uncompressed", size, best * 1000))
