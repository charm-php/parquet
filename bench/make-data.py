"""
Writes the benchmark files into bench/data/ (git-ignored) with pyarrow. About one million values per workload, each with snappy and zstd:

    python3 -m venv research/venv && research/venv/bin/pip install pyarrow
    research/venv/bin/python bench/make-data.py

The data is deterministic (seeded). One row group per file of 500 000 rows (list and wide workloads: about 500 000 values).
"""
import os
import random
import sys
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
os.makedirs(OUT, exist_ok=True)
r = random.Random(20260101)
N = 1_000_000


def write(name, table, row_group_size=500_000, **kw):
    for codec in ["snappy", "zstd"]:
        path = os.path.join(OUT, "%s.%s.parquet" % (name, codec))
        pq.write_table(table, path, compression=codec, row_group_size=row_group_size, **kw)
        print("%-34s %9d bytes" % (os.path.basename(path), os.path.getsize(path)))


def lt(t):
    return pa.list_(pa.field("element", t))


ints = [r.randint(-(2**40), 2**40) for _ in range(N)]
write("int64-plain", pa.table({"v": pa.array(ints, type=pa.int64())}), use_dictionary=False)
write("int64-dict", pa.table({"v": pa.array([r.randint(0, 999) for _ in range(N)], type=pa.int64())}), use_dictionary=True)
write("int64-delta", pa.table({"v": pa.array([i * 3 + r.randint(0, 5) for i in range(N)], type=pa.int64())}), use_dictionary=False, column_encoding={"v": "DELTA_BINARY_PACKED"})
write("double", pa.table({"v": pa.array([r.random() * 1000 for _ in range(N)], type=pa.float64())}), use_dictionary=False)
write("str-lowcard", pa.table({"v": pa.array(["category-%03d" % r.randint(0, 199) for _ in range(N)])}), use_dictionary=True)
write("str-highcard", pa.table({"v": pa.array(["%08x-%s" % (r.getrandbits(32), "x" * r.randint(0, 20)) for _ in range(N)])}), use_dictionary=False)
write("str-delta", pa.table({"v": pa.array(sorted("https://example.org/%d/%d" % (r.randint(0, 999), r.randint(0, 99999)) for _ in range(N)))}),
      use_dictionary=False, column_encoding={"v": "DELTA_BYTE_ARRAY"})
write("opt-10pct-null", pa.table({"v": pa.array([None if r.random() < 0.1 else r.randint(0, 10**9) for _ in range(N)], type=pa.int64())}), use_dictionary=False)
write("opt-90pct-null", pa.table({"v": pa.array([None if r.random() < 0.9 else r.randint(0, 10**9) for _ in range(N)], type=pa.int64())}), use_dictionary=False)
write("decimal", pa.table({"v": pa.array([Decimal(r.randint(-10**11, 10**11)).scaleb(-2) for _ in range(N)], type=pa.decimal128(14, 2))}), use_dictionary=False)
write("timestamp", pa.table({"v": pa.array([1_600_000_000_000_000 + r.randint(0, 10**14) for _ in range(N)], type=pa.timestamp("us", tz="UTC"))}), use_dictionary=False)
write("list-int32-tokens", pa.table({"v": pa.array([[r.randint(0, 50000) for _ in range(r.randint(8, 12))] for _ in range(100_000)], type=lt(pa.int32()))}), row_group_size=50_000)
write("list-float-128", pa.table({"v": pa.array([[r.random() for _ in range(128)] for _ in range(8_000)], type=lt(pa.float32()))}), row_group_size=4_000, use_dictionary=False)
write("struct-3", pa.table({"v": pa.array([{"a": r.randint(0, 999), "b": "name%d" % r.randint(0, 500), "c": r.random()} for _ in range(N // 3)],
                                         type=pa.struct([pa.field("a", pa.int32()), pa.field("b", pa.string()), pa.field("c", pa.float64())]))}), row_group_size=170_000)
wide = {}
for c in range(50):
    k = c % 5
    if k == 0:
        wide["c%02d" % c] = pa.array([r.randint(0, 10**6) for _ in range(20_000)], type=pa.int64())
    elif k == 1:
        wide["c%02d" % c] = pa.array([r.random() for _ in range(20_000)])
    elif k == 2:
        wide["c%02d" % c] = pa.array(["s%d" % r.randint(0, 300) for _ in range(20_000)])
    elif k == 3:
        wide["c%02d" % c] = pa.array([None if r.random() < 0.2 else r.randint(0, 1000) for _ in range(20_000)], type=pa.int32())
    else:
        wide["c%02d" % c] = pa.array([r.random() < 0.5 for _ in range(20_000)])
write("wide-50", pa.table(wide), row_group_size=10_000)
