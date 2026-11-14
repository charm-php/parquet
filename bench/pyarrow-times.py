"""
For context only: the time pyarrow needs for the same reads on the same files, a different runtime doing different work (native decoding
into Arrow arrays, then a conversion to Python objects). Not a comparison of like with like.

    research/venv/bin/python bench/pyarrow-times.py [runs]

"to_pylist" is pq.read_table(...) followed by .to_pylist() (Python objects, what charm/parquet returns); "arrow only" stops at the Arrow table.
"""
import os
import sys
import time

import pyarrow.parquet as pq

DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
RUNS = int(sys.argv[1]) if len(sys.argv) > 1 else 5


def best(fn):
    fn()
    b = float("inf")
    for _ in range(RUNS):
        t = time.perf_counter()
        fn()
        b = min(b, time.perf_counter() - t)
    return b * 1000


names = ["int64-plain", "int64-dict", "int64-delta", "double", "str-lowcard", "str-highcard", "str-delta", "opt-10pct-null", "opt-90pct-null",
         "decimal", "timestamp", "list-int32-tokens", "list-float-128", "struct-3"]
print("pyarrow %s; ms, best of %d; snappy files" % (__import__("pyarrow").__version__, RUNS))
print("%-30s %12s %12s" % ("workload", "arrow only", "to_pylist"))
for n in names:
    path = os.path.join(DIR, n + ".snappy.parquet")
    a = best(lambda: pq.read_table(path))
    b = best(lambda: pq.read_table(path).column("v").to_pylist())
    print("%-30s %12.1f %12.1f" % (n, a, b))
path = os.path.join(DIR, "wide-50.snappy.parquet")
print("%-30s %12.1f %12.1f" % ("wide-50, all columns as rows", best(lambda: pq.read_table(path)), best(lambda: pq.read_table(path).to_pylist())))
cols = ["c00", "c02", "c03"]
print("%-30s %12.1f %12.1f" % ("wide-50, 3 columns as rows", best(lambda: pq.read_table(path, columns=cols)), best(lambda: pq.read_table(path, columns=cols).to_pylist())))
