"""
Regenerates the Parquet fixtures in tests/fixtures/ and their expected values (tests/fixtures/expected/*.json) with pyarrow:

    research/venv/bin/python tests/fixtures/generate.py

Each fixture NAME.parquet has NAME.json next to it (in expected/) with
  "rows"       the number of rows
  "columns"    path -> {"physical": ..., "values": [...]} in the canonical text form of tests/tools/oracle.py (what pyarrow reads
               from the file, converted back to the physical values)
  "metadata"   what pyarrow reports about the footer: num_rows, row groups, schema leaves, per column chunk codec and encodings
or, for a file that uses something the library does not read yet, "unsupported": the exact message expected.
The data is deterministic (seeded random).
"""
import json
import os
import random
import struct
import sys

import pyarrow as pa
import pyarrow.parquet as pq

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "tools"))
import oracle  # noqa: E402

OUT = HERE
EXP = os.path.join(HERE, "expected")
os.makedirs(EXP, exist_ok=True)


def metadata_of(path):
    pf = pq.ParquetFile(path)
    m = pf.metadata
    sch = pf.schema
    leaves = []
    for i in range(len(sch)):
        c = sch.column(i)
        leaves.append({
            "path": c.path,
            "physical": c.physical_type,
            "maxDef": c.max_definition_level,
            "maxRep": c.max_repetition_level,
            "converted": str(c.converted_type) if c.converted_type != "NONE" else None,
            "logical": str(c.logical_type) if str(c.logical_type) != "None" else None,
        })
    groups = []
    for g in range(m.num_row_groups):
        rg = m.row_group(g)
        cols = []
        for c in range(rg.num_columns):
            cc = rg.column(c)
            cols.append({
                "path": cc.path_in_schema,
                "codec": cc.compression,
                "encodings": sorted(cc.encodings),
                "numValues": cc.num_values,
                "hasDictionaryPage": cc.has_dictionary_page,
                "nullCount": cc.statistics.null_count if cc.is_stats_set and cc.statistics.has_null_count else None,
            })
        groups.append({"numRows": rg.num_rows, "columns": cols})
    return {
        "version": m.format_version,
        "numRows": m.num_rows,
        "numRowGroups": m.num_row_groups,
        "createdBy": m.created_by,
        "leaves": leaves,
        "rowGroups": groups,
        "keyValueKeys": sorted(k.decode() for k in (m.metadata or {}).keys()),
    }


def expected_values(path):
    pf = pq.ParquetFile(path, coerce_int96_timestamp_unit="us")
    table = pf.read()
    sch = pf.schema
    cols = {}
    for i in range(len(sch)):
        c = sch.column(i)
        if c.max_repetition_level != 0 or c.max_definition_level > 1:
            continue
        arr = oracle.leaf_array(table, c.path.split("."))
        vals = oracle.to_physical(arr, c.physical_type, c.length if c.physical_type == "FIXED_LEN_BYTE_ARRAY" else None)
        # tuples (decimals in BYTE_ARRAY, INT96) become the canonical hex strings the library produces
        out = []
        for v in vals:
            if isinstance(v, tuple) and v[0] == "int96us":  # (the fixtures only hold whole microseconds)
                out.append(struct.pack("<qI", v[1] * 1000, v[2]).hex())
            elif isinstance(v, tuple):
                raise SystemExit("fixture column %s has a value that needs oracle-side normalisation" % c.path)
            else:
                out.append(v)
        cols[c.path] = {"physical": c.physical_type, "values": out}
    return {"rows": table.num_rows, "columns": cols}


SHARED = {}


def logical_values(path):
    """What the library's LOGICAL mode must return, in the canonical text of oracle.to_logical (an independent Python formatting)."""
    pf = pq.ParquetFile(path, coerce_int96_timestamp_unit="us")
    table = pf.read()
    sch = pf.schema
    cols = {}
    for i in range(len(sch)):
        c = sch.column(i)
        if c.max_repetition_level != 0 or c.max_definition_level > 1:
            continue
        arr = oracle.leaf_array(table, c.path.split("."))
        cols[c.path] = {"physical": c.physical_type, "values": oracle.to_logical(arr, c.physical_type, None)}
    return cols


def statistics_of(path):
    """True min and max of each column computed by pyarrow from the data (not from the footer), as canonical logical text, plus the
    footer's null count. Skipped for columns with NaN, and for types pyarrow cannot take min/max of."""
    import pyarrow.compute as pc
    pf = pq.ParquetFile(path, coerce_int96_timestamp_unit="us")
    sch = pf.schema
    out = {}
    for g in range(pf.metadata.num_row_groups):
        tbl = pf.read_row_group(g)
        rg = pf.metadata.row_group(g)
        for i in range(len(sch)):
            c = sch.column(i)
            if c.max_repetition_level != 0 or c.max_definition_level > 1 or c.physical_type in ("INT96", "BOOLEAN"):
                continue
            arr = oracle.leaf_array(tbl, c.path.split("."))
            cc = rg.column(i)
            if not cc.is_stats_set or not cc.statistics.has_min_max:
                continue
            try:
                if pa.types.is_floating(arr.type) and pc.any(pc.is_nan(arr)).as_py():
                    continue
                mm = pc.min_max(arr)
                lo, hi = mm["min"], mm["max"]
                if not lo.is_valid:
                    continue
                def one(sc):
                    return oracle.to_logical(pa.chunked_array([pa.repeat(sc, 1)]), c.physical_type, None)[0]
                out["%d:%s" % (g, c.path)] = {"min": one(lo), "max": one(hi), "nullCount": cc.statistics.null_count}
            except Exception:  # noqa: BLE001 (an Arrow type without min_max or without a python conversion)
                continue
    return out


def nested_trees(path):
    """The assembled shapes pyarrow reads, as canonical trees (oracle.top_trees) in every mode: {"physical", "logical", "pairs"}."""
    pf = pq.ParquetFile(path, coerce_int96_timestamp_unit="us")
    table = pf.read()
    return {"physical": oracle.top_trees(table, pf, "physical"), "logical": oracle.top_trees(table, pf, "logical"),
            "pairs": oracle.top_trees(table, pf, "logical", pairs=True)}


def save(name, table_or_none, unsupported=None, shared=None, brotli=False, logical=False, nested=False, **kw):
    """`shared`: files written from the same table keep the values once; the others say "sameValuesAs" (and the test checks the rows)."""
    path = os.path.join(OUT, name + ".parquet")
    if table_or_none is not None:
        pq.write_table(table_or_none, path, **kw)
    exp = {"metadata": metadata_of(path)}
    if unsupported:
        exp["unsupported"] = unsupported
        exp["rows"] = exp["metadata"]["numRows"]
        exp["columns"] = {}
    elif shared is not None and shared in SHARED:
        exp["sameValuesAs"] = SHARED[shared]
    else:
        exp.update(expected_values(path))
        if shared is not None:
            SHARED[shared] = name
    first = shared is None or shared not in SHARED
    if nested and first:
        exp["trees"] = nested_trees(path)
    if logical and not unsupported and first:
        exp["columnsLogical"] = logical_values(path)
    if not unsupported:
        stats = statistics_of(path)
        if stats:
            exp["statistics"] = stats
    with open(os.path.join(EXP, name + ".json"), "w") as f:
        json.dump(exp, f, separators=(",", ":"))
    print("%-36s %8d bytes" % (name, os.path.getsize(path)))


rnd = random.Random(20260101)


def pick(pool, i, p_pool=0.5):
    return rnd.choice(pool) if rnd.random() < p_pool else None


def matrix_table(n=250):
    """Eight types, each as a required and as an optional column (about a quarter of the optional values are null)."""
    ints = [0, 1, -1, 2**31 - 1, -(2**31), 7, 7, 7, 1000]
    longs = [0, -1, 2**63 - 1, -(2**63), 123456789012, 5, 5, 5]
    floats = [0.0, -0.0, 1.5, -2.25, float("inf"), float("-inf"), float("nan"), 3.4028234663852886e38, 1e-45]
    doubles = [0.0, -0.0, 0.1, -1e300, float("inf"), float("nan"), 5e-324, 1.7976931348623157e308]
    words = ["", "a", "alpha", "beta", "gamma", "déjà vu", "日本語", "x" * 300, "same", "same"]
    blobs = [b"", b"\x00", b"\x00\x01\xff", bytes(range(256)), b"\xff\xfe", b"same", b"same"]

    def gen(kind):
        v = []
        for i in range(n):
            if kind == "bool":
                v.append(rnd.random() < 0.3 if i % 5 else True)
            elif kind == "int32":
                v.append(rnd.choice(ints) if i % 3 else rnd.randint(-(2**31), 2**31 - 1))
            elif kind == "int64":
                v.append(rnd.choice(longs) if i % 3 else rnd.randint(-(2**63), 2**63 - 1))
            elif kind == "float32":
                v.append(rnd.choice(floats) if i % 3 else rnd.uniform(-1e6, 1e6))
            elif kind == "float64":
                v.append(rnd.choice(doubles) if i % 3 else rnd.uniform(-1e12, 1e12))
            elif kind == "string":
                v.append(rnd.choice(words) if i % 4 else "unique-%d-%d" % (i, rnd.randint(0, 10**9)))
            elif kind == "binary":
                v.append(rnd.choice(blobs) if i % 4 else bytes(rnd.randint(0, 255) for _ in range(rnd.randint(0, 40))))
            elif kind == "fixed":
                v.append(rnd.choice([b"abcde", b"\x00\x00\x00\x00\x00", b"\xff\xff\xff\xff\xff"]) if i % 3 else bytes(rnd.randint(0, 255) for _ in range(5)))
        return v

    types = {"bool": pa.bool_(), "int32": pa.int32(), "int64": pa.int64(), "float32": pa.float32(), "float64": pa.float64(),
             "string": pa.string(), "binary": pa.binary(), "fixed": pa.binary(5)}
    fields, arrays = [], []
    for kind, t in types.items():
        vals = gen(kind)
        fields.append(pa.field("r_" + kind, t, nullable=False))
        arrays.append(pa.array(vals, type=t))
        opt = [None if rnd.random() < 0.25 else x for x in gen(kind)]
        fields.append(pa.field("o_" + kind, t))
        arrays.append(pa.array(opt, type=t))
    return pa.Table.from_arrays(arrays, schema=pa.schema(fields))


matrix = matrix_table()
for codec in ["none", "snappy", "gzip", "zstd"]:
    for version in ["1.0", "2.0"]:
        for dictionary in [True, False]:
            dp = "v2" if version == "2.0" else "v1"
            save("matrix-%s-%s-%s" % (codec, dp, "dict" if dictionary else "plain"), matrix,
                 shared="matrix", compression=codec, data_page_version=version, use_dictionary=dictionary, data_page_size=1 << 20)

# several pages per chunk: tiny data pages, with a dictionary and without
pages_table = matrix_table(250)
for version in ["1.0", "2.0"]:
    save("pages-%s-dict" % ("v2" if version == "2.0" else "v1"), pages_table, shared="pages", compression="snappy", data_page_version=version, data_page_size=512, write_batch_size=20, use_dictionary=True)
    save("pages-%s-plain" % ("v2" if version == "2.0" else "v1"), pages_table, shared="pages", compression="none", data_page_version=version, data_page_size=512, write_batch_size=20, use_dictionary=False)

# dictionary fallback: the dictionary fills up (tiny dictionary page limit) and the rest of the chunk is PLAIN
many = pa.table({
    "word": pa.array(["w%06d-%d" % (rnd.randint(0, 10**6), i) for i in range(1500)]),
    "num": pa.array([rnd.randint(0, 10**9) for _ in range(1500)], type=pa.int64()),
    "few": pa.array([rnd.choice(["a", "b", "c"]) for _ in range(1500)]),
})
for version in ["1.0", "2.0"]:
    save("dict-fallback-%s" % ("v2" if version == "2.0" else "v1"), many, compression="snappy", data_page_version=version, data_page_size=2048, write_batch_size=50,
         dictionary_pagesize_limit=4096, use_dictionary=True)

# several row groups
save("rowgroups", matrix_table(300), row_group_size=100, compression="snappy")

# nulls at the start, at the end, all null, no null (optional columns)
n = 400
save("null-patterns", pa.table({
    "start": pa.array([None] * 150 + list(range(n - 150)), type=pa.int32()),
    "end": pa.array(list(range(n - 150)) + [None] * 150, type=pa.int64()),
    "all_null": pa.array([None] * n, type=pa.string()),
    "none_null": pa.array(["v%d" % (i % 7) for i in range(n)]),
    "alternate": pa.array([None if i % 2 else float(i) for i in range(n)], type=pa.float64()),
    "all_null_bool": pa.array([None] * n, type=pa.bool_()),
    "bools": pa.array([None if i % 3 == 0 else (i % 2 == 0) for i in range(n)], type=pa.bool_()),
}), compression="none")
save("null-patterns-v2", pa.table({
    "start": pa.array([None] * 150 + list(range(n - 150)), type=pa.int32()),
    "all_null": pa.array([None] * n, type=pa.string()),
    "end": pa.array(list(range(n - 150)) + [None] * 150, type=pa.int64()),
}), compression="zstd", data_page_version="2.0", use_dictionary=False)

# empty files
empty_schema = pa.schema([pa.field("a", pa.int64()), pa.field("b", pa.string())])
save("empty-no-rowgroups", pa.table({"a": pa.array([], type=pa.int64()), "b": pa.array([], type=pa.string())}))
with pq.ParquetWriter(os.path.join(OUT, "empty-zero-row-group.parquet"), empty_schema) as w:
    w.write_table(pa.table({"a": pa.array([], type=pa.int64()), "b": pa.array([], type=pa.string())}), row_group_size=1)
    w.write_table(pa.table({"a": pa.array([1, 2], type=pa.int64()), "b": pa.array(["x", None], type=pa.string())}))
    w.write_table(pa.table({"a": pa.array([], type=pa.int64()), "b": pa.array([], type=pa.string())}))
save("empty-zero-row-group", None)

# logical types, stored as their physical types
D = __import__("decimal").Decimal
__import__("decimal").getcontext().prec = 120
save("logical", pa.table({
    "u8": pa.array([0, 255, 7], type=pa.uint8()), "u16": pa.array([0, 65535, 7], type=pa.uint16()),
    "u32": pa.array([0, 2**32 - 1, 7], type=pa.uint32()), "u64": pa.array([0, 2**64 - 1, 7], type=pa.uint64()),
    "i8": pa.array([-128, 127, 0], type=pa.int8()), "i16": pa.array([-32768, 32767, 0], type=pa.int16()),
    "date": pa.array([0, 19000, -5], type=pa.date32()), "t32": pa.array([0, 86399999, 5], type=pa.time32("ms")),
    "t64": pa.array([0, 86399999999, 5], type=pa.time64("us")),
    "ts": pa.array([0, 1700000000123456, None], type=pa.timestamp("us", tz="UTC")),
    "dec_flba": pa.array([D("123.45"), D("-999.99"), None], type=pa.decimal128(10, 2)),
    "f16": pa.array([0.5, float("nan"), -0.0], type=pa.float16()),
}), logical=True, compression="snappy")
save("decimal-int", pa.table({
    "d9": pa.array([D("1234567.89"), None, D("-9999999.99"), D("9999999.99"), D("0.00"), D("-0.01")], type=pa.decimal128(9, 2)),
    "d18": pa.array([D("12345678901234.5678"), D("-1.0000"), D("99999999999999.9999"), D("-99999999999999.9999"), None, D("0.0001")], type=pa.decimal128(18, 4)),
    "d4": pa.array([D("99.99"), D("-99.99"), None, D("0.05"), D("0.00"), D("1.00")], type=pa.decimal128(4, 2)),
}), store_decimal_as_integer=True, logical=True)
save("int96", pa.table({"ts": pa.array([0, 1700000000123456000, None, -86400 * 10**9, 951782400000000000], type=pa.timestamp("ns"))}), use_deprecated_int96_timestamps=True, logical=True)

# decimals of every stored size: FIXED_LEN_BYTE_ARRAY of 1 to 16 bytes (precisions 2, 4, ..., 38) and of 21 and 32 bytes (decimal256)
dcols = {}
for prec in [2, 4, 6, 9, 11, 14, 16, 18, 21, 23, 26, 28, 31, 33, 35, 38]:
    mx = D(10**prec - 1).scaleb(-(prec // 3))
    dcols["p%d" % prec] = pa.array([mx, -mx, D(0).scaleb(-(prec // 3)), D(1).scaleb(-(prec // 3)), D(-1).scaleb(-(prec // 3)), None, D(12345 % (10**prec)).scaleb(-(prec // 3))],
                                    type=pa.decimal128(prec, prec // 3))
dcols["scale0"] = pa.array([D(123), D(-4), D(0), None, D(10**20), D(-(10**20))], type=pa.decimal128(38, 0)) if False else pa.array([D(123), D(-4), D(0), None, D(10**20), D(-(10**20)), D(1) , D(2)], type=pa.decimal128(38, 0))
dcols["frac_only"] = pa.array([D("0.0000000001"), D("-0.0000000001"), D("0.5000000000"), None, D("0E-10"), D("0.1234567890"), D("-0.9999999999"), D("0.0000000000")], type=pa.decimal128(10, 10))
dcols["d256"] = pa.array([D(10**75 - 1).scaleb(-20), -D(10**75 - 1).scaleb(-20), D(0), D(1).scaleb(-20), D(-1).scaleb(-20), None, D(2**200).scaleb(-20), -D(2**200).scaleb(-20)],
                         type=pa.decimal256(76, 20))
dcols["d50"] = pa.array([D(10**49).scaleb(-5), -D(10**49).scaleb(-5), D("1.00000"), D("-0.00001"), None, D(0), D(7), D(-7)], type=pa.decimal256(50, 5))
n_dec = max(len(v) for v in dcols.values())
dcols = {k: pa.concat_arrays([v, pa.nulls(n_dec - len(v), type=v.type)]) for k, v in dcols.items()}
save("decimals", pa.table(dcols), logical=True)

# dates, times and timestamps in every unit, around the epoch, far away from it, and across the proleptic Gregorian edge cases
INT32_MIN, INT32_MAX = -(2**31), 2**31 - 1
dates = [0, 1, -1, 59, 60, 365, 366, 18262, 19000, -719528, -719162, -719163, -719893, -678941, -25508, 2932896, 2932897, INT32_MIN, INT32_MAX, 11016, -141427]
ts_us = [0, 1, -1, 1700000000123456, -1700000000123456, 951782400000000, 951868799999999, -62135596800000000, -62135596800000001, 253402300799999999, 253402300800000000,
         -2**62, 2**62, 2**63 - 1, -(2**63), 86399999999, 86400000000, -86400000000, -86400000001]
ts_ms = [0, 1, -1, 1700000000123, -1700000000123, 951782400000, 253402300799999, 253402300800000, -62135596800000, -62135596800001, 2**62, -(2**62), 2**63 - 1, -(2**63), 86399999, -86400001]
ts_ns = [0, 1, -1, 1700000000123456789, -1700000000123456789, 951782400000000000, 2**63 - 1, -(2**63), 86399999999999, 86400000000000, -86400000000001, 1234567890123456789]
def padn(l, n):
    return l + [None] * (n - len(l))
N = max(len(dates), len(ts_us), len(ts_ms), len(ts_ns))
save("logical-edges", pa.table({
    "date": pa.array(padn(dates, N), type=pa.date32()),
    "t_ms": pa.array(padn([0, 1, 999, 1000, 86399999, 43200000, 3661001, 86400000], N), type=pa.time32("ms")),
    "t_us": pa.array(padn([0, 1, 999999, 1000000, 86399999999, 43200000000, 3661000001], N), type=pa.time64("us")),
    "t_ns": pa.array(padn([0, 1, 999999999, 86399999999999, 43200000000000, 3661000000001], N), type=pa.time64("ns")),
    "ts_ms_utc": pa.array(padn(ts_ms, N), type=pa.timestamp("ms", tz="UTC")),
    "ts_us_utc": pa.array(padn(ts_us, N), type=pa.timestamp("us", tz="UTC")),
    "ts_ns_utc": pa.array(padn(ts_ns, N), type=pa.timestamp("ns", tz="UTC")),
    "ts_ms": pa.array(padn(ts_ms, N), type=pa.timestamp("ms")),
    "ts_us": pa.array(padn(ts_us, N), type=pa.timestamp("us")),
    "ts_ns": pa.array(padn(ts_ns, N), type=pa.timestamp("ns")),
    "u8": pa.array(padn([0, 255, 128, 1], N), type=pa.uint8()),
    "u16": pa.array(padn([0, 65535, 32768], N), type=pa.uint16()),
    "u32": pa.array(padn([0, 2**32 - 1, 2**31, 1], N), type=pa.uint32()),
    "u64": pa.array(padn([0, 2**64 - 1, 2**63, 2**63 - 1, 10**19, 18446744073709551610, 1], N), type=pa.uint64()),
    "i8": pa.array(padn([-128, 127, 0, -1], N), type=pa.int8()),
    "i16": pa.array(padn([-32768, 32767, 0, -1], N), type=pa.int16()),
    "i32": pa.array(padn([INT32_MIN, INT32_MAX, 0, -1], N), type=pa.int32()),
    "i64": pa.array(padn([-(2**63), 2**63 - 1, 0, -1], N), type=pa.int64()),
    "uuid": pa.array(padn([bytes(range(16)), b"\xff" * 16, b"\x00" * 16, bytes.fromhex("123e4567e89b12d3a456426614174000")], N), type=pa.uuid()),
    "f16": pa.array(padn([0.0, -0.0, 1.0, 65504.0, 6.103515625e-05, 5.960464477539063e-08, float("inf"), float("-inf"), float("nan"), 0.333251953125, -2.5], N), type=pa.float16()),
    "f32": pa.array(padn([0.0, -0.0, 1.5, float("inf"), float("nan"), 3.4028234663852886e38], N), type=pa.float32()),
    "str": pa.array(padn(["", "abc", "\u00e9\u65e5\u672c", "x" * 50], N)),
}), logical=True)

# nested schemas: they parse; their leaves under lists and maps are not readable yet; plain struct leaves are
nested_schema = pa.schema([pa.field("id", pa.int64()), pa.field("st", pa.struct([pa.field("x", pa.int32()), pa.field("y", pa.string())])),
    pa.field("reqst", pa.struct([pa.field("x", pa.int32(), nullable=False)])), pa.field("reqgroup", pa.struct([pa.field("x", pa.int32())]), nullable=False),
    pa.field("lst", pa.list_(pa.int32())), pa.field("mp", pa.map_(pa.string(), pa.int64())),
    pa.field("deep", pa.struct([pa.field("a", pa.struct([pa.field("b", pa.int32())]))]))])
save("nested", pa.table({
    "id": pa.array([1, 2, 3], type=pa.int64()),
    "st": pa.array([{"x": 1, "y": "a"}, None, {"x": 3, "y": None}], type=pa.struct([pa.field("x", pa.int32()), pa.field("y", pa.string())])),
    "reqst": pa.array([{"x": 1}, {"x": 2}, {"x": 3}], type=pa.struct([pa.field("x", pa.int32(), nullable=False)])),
    "reqgroup": pa.array([{"x": 10}, {"x": None}, {"x": 30}], type=pa.struct([pa.field("x", pa.int32())])),
    "lst": pa.array([[1, 2], None, []], type=pa.list_(pa.int32())),
    "mp": pa.array([[("k", 1)], None, []], type=pa.map_(pa.string(), pa.int64())),
    "deep": pa.array([{"a": {"b": 5}}, {"a": None}, None], type=pa.struct([pa.field("a", pa.struct([pa.field("b", pa.int32())]))])),
}, schema=nested_schema), nested=True)

# key-value metadata, statistics, created_by
t = matrix_table(50).replace_schema_metadata({"owner": "tests", "note": "kéy"})
save("kv-metadata", t, write_statistics=True)

# a footer larger than the 64 KiB the reader speculatively reads from the end of the file
save("big-footer", matrix_table(20).replace_schema_metadata({"big": "x" * 33000}), compression="none")

# DELTA_BINARY_PACKED: sequences, constants, random full-range values (64-bit deltas), 32-bit wraparound, nulls, several pages
def delta_table(n, seed):
    r = random.Random(seed)
    lo32, hi32 = -(2**31), 2**31 - 1
    lo64, hi64 = -(2**63), 2**63 - 1
    cols = {
        "seq32": pa.array(list(range(n)), type=pa.int32()),
        "seq64": pa.array([10**12 + 3 * i for i in range(n)], type=pa.int64()),
        "const32": pa.array([7] * n, type=pa.int32()),
        "const64": pa.array([-5] * n, type=pa.int64()),
        "rand32": pa.array([r.randint(lo32, hi32) for _ in range(n)], type=pa.int32()),
        "rand64": pa.array([r.randint(lo64, hi64) for _ in range(n)], type=pa.int64()),
        "extreme32": pa.array([lo32 if i % 2 else hi32 for i in range(n)], type=pa.int32()),
        "extreme64": pa.array([lo64 if i % 2 else hi64 for i in range(n)], type=pa.int64()),
        "small64": pa.array([r.randint(-3, 3) for _ in range(n)], type=pa.int64()),
        "wide64": pa.array([r.choice([0, 2**62, -(2**62), 2**40, 12345]) for _ in range(n)], type=pa.int64()),
        "opt32": pa.array([None if r.random() < 0.3 else r.randint(-1000, 1000) for _ in range(n)], type=pa.int32()),
        "opt64": pa.array([None if r.random() < 0.3 else r.randint(lo64, hi64) for _ in range(n)], type=pa.int64()),
    }
    return pa.table(cols)


enc_all = lambda t, e: {c: e for c in t.column_names}  # noqa: E731
for n in [1, 2, 3, 127, 128, 129, 130, 257, 700]:
    t = delta_table(n, n)
    for version in ["1.0", "2.0"]:
        if n not in (1, 129, 700) and version == "2.0":
            continue
        save("delta-int-n%d-%s" % (n, "v2" if version == "2.0" else "v1"), t, use_dictionary=False, column_encoding=enc_all(t, "DELTA_BINARY_PACKED"),
             data_page_version=version, compression="none", write_batch_size=100 if n > 200 else 1024, data_page_size=2000 if n > 200 else 1 << 20)
save("delta-int-zero-rows", delta_table(0, 0), use_dictionary=False, column_encoding=enc_all(delta_table(0, 0), "DELTA_BINARY_PACKED"))

# DELTA_LENGTH_BYTE_ARRAY and DELTA_BYTE_ARRAY (prefix lengths): sorted URLs, empty strings, binary with NUL bytes, fixed-length
r = random.Random(99)
urls = sorted("https://example.org/%s/%d" % (r.choice(["a", "ab", "abc", "users", "users/42/posts"]), r.randint(0, 5000)) for _ in range(300))
strs = pa.table({
    "urls": pa.array(urls),
    "opt_urls": pa.array([None if r.random() < 0.3 else u for u in urls]),
    "words": pa.array([r.choice(["", "a", "alpha", "alphabet", "alp", "\u00e9t\u00e9", "x" * 200]) for _ in range(300)]),
    "blobs": pa.array([bytes(r.randint(0, 255) for _ in range(r.randint(0, 12))) for _ in range(300)], type=pa.binary()),
    "fixed": pa.array([bytes(r.choice([0, 1, 255, r.randint(0, 255)]) for _ in range(5)) for _ in range(300)], type=pa.binary(5)),
    "opt_fixed": pa.array([None if r.random() < 0.4 else b"abc%02d" % r.randint(0, 20) for _ in range(300)], type=pa.binary(5)),
})
for version in ["1.0", "2.0"]:
    v = "v2" if version == "2.0" else "v1"
    save("delta-length-byte-array-" + v, strs.select(["urls", "opt_urls", "words", "blobs"]), shared="dlba", use_dictionary=False,
         column_encoding={"urls": "DELTA_LENGTH_BYTE_ARRAY", "opt_urls": "DELTA_LENGTH_BYTE_ARRAY", "words": "DELTA_LENGTH_BYTE_ARRAY", "blobs": "DELTA_LENGTH_BYTE_ARRAY"},
         data_page_version=version, write_batch_size=50, data_page_size=1500)
    save("delta-byte-array-" + v, strs, shared="dba", use_dictionary=False, column_encoding={c: "DELTA_BYTE_ARRAY" for c in strs.column_names},
         data_page_version=version, write_batch_size=50, data_page_size=1500)

# BYTE_STREAM_SPLIT: float, double, int32, int64, fixed-length byte arrays; with nulls
r = random.Random(7)
bss = pa.table({
    "f32": pa.array([r.choice([0.0, -0.0, 1.5, float("nan"), float("inf"), r.uniform(-1e6, 1e6)]) for _ in range(500)], type=pa.float32()),
    "f64": pa.array([r.choice([0.0, float("-inf"), 5e-324, r.uniform(-1e12, 1e12)]) for _ in range(500)], type=pa.float64()),
    "i32": pa.array([r.randint(-(2**31), 2**31 - 1) for _ in range(500)], type=pa.int32()),
    "i64": pa.array([r.randint(-(2**63), 2**63 - 1) for _ in range(500)], type=pa.int64()),
    "flba": pa.array([bytes(r.randint(0, 255) for _ in range(7)) for _ in range(500)], type=pa.binary(7)),
    "o_f64": pa.array([None if r.random() < 0.3 else r.random() for _ in range(500)], type=pa.float64()),
    "o_i32": pa.array([None if r.random() < 0.3 else r.randint(-100, 100) for _ in range(500)], type=pa.int32()),
})
for version in ["1.0", "2.0"]:
    save("byte-stream-split-" + ("v2" if version == "2.0" else "v1"), bss, shared="bss", use_dictionary=False, column_encoding=enc_all(bss, "BYTE_STREAM_SPLIT"),
         data_page_version=version, write_batch_size=100, data_page_size=2000, compression="none")
save("byte-stream-split-zstd", bss, shared="bss", use_dictionary=False, column_encoding=enc_all(bss, "BYTE_STREAM_SPLIT"), compression="zstd")

# codecs: LZ4_RAW (pyarrow writes the raw block format for "lz4"), and brotli (read with charm/brotli, or ext-brotli when loaded)
tt = pa.table({"i": pa.array(list(range(2000)), type=pa.int64()), "s": pa.array(["v%d-%s" % (i, "x" * (i % 30)) for i in range(2000)]),
               "o": pa.array([None if i % 3 == 0 else i * 7 % 1000 for i in range(2000)], type=pa.int32())})
for version in ["1.0", "2.0"]:
    save("lz4raw-" + ("v2" if version == "2.0" else "v1"), tt, shared="lz4", compression="lz4", data_page_version=version, write_batch_size=300, data_page_size=4000)
save("brotli", tt, shared="lz4", brotli=True, compression="brotli")

# ---- nested columns: lists, structs, maps, in data pages v1 and v2, with tiny pages so that rows span pages ----
def nsave(name, table, **kw):
    for version in ["1.0", "2.0"]:
        save("nest-%s-%s" % (name, "v2" if version == "2.0" else "v1"), table, shared="nest-" + name, nested=True,
             data_page_version=version, data_page_size=256, write_batch_size=7, **kw)


def lt(t, element_nullable=True):
    return pa.list_(pa.field("element", t, nullable=element_nullable))


r = random.Random(4242)


def maybe(x, p=0.25):
    return None if r.random() < p else x


def rand_list(gen, p_null=0.2, p_empty=0.2, p_null_el=0.2, maxlen=6):
    if r.random() < p_null:
        return None
    if r.random() < p_empty:
        return []
    return [None if r.random() < p_null_el else gen() for _ in range(r.randint(1, maxlen))]


n = 120
nsave("list", pa.table({
    "id": pa.array(range(n), type=pa.int32()),
    "li": pa.array([rand_list(lambda: r.randint(-1000, 1000)) for _ in range(n)], type=lt(pa.int32())),
    "li_req": pa.array([[r.randint(0, 9) for _ in range(r.randint(0, 5))] for _ in range(n)], type=lt(pa.int32(), False)),
    "li_req_null": pa.array([None if r.random() < 0.3 else [r.randint(0, 9) for _ in range(r.randint(0, 5))] for _ in range(n)], type=lt(pa.int32(), False)),
    "ls": pa.array([rand_list(lambda: "s%d" % r.randint(0, 50)) for _ in range(n)], type=lt(pa.string())),
    "lf": pa.array([rand_list(lambda: r.choice([0.5, -2.25, float("nan"), float("inf")])) for _ in range(n)], type=lt(pa.float64())),
    "lb": pa.array([rand_list(lambda: r.random() < 0.5) for _ in range(n)], type=lt(pa.bool_())),
    "lli": pa.array([rand_list(lambda: rand_list(lambda: r.randint(0, 99), maxlen=4), maxlen=4) for _ in range(n)], type=lt(lt(pa.int64()))),
    "ld": pa.array([rand_list(lambda: __import__("decimal").Decimal(r.randint(-99999, 99999)).scaleb(-2), maxlen=4) for _ in range(n)], type=lt(pa.decimal128(7, 2))),
    "lt": pa.array([rand_list(lambda: r.randint(0, 10**12), maxlen=3) for _ in range(n)], type=lt(pa.timestamp("us", tz="UTC"))),
}))

st_t = pa.struct([pa.field("a", pa.int32()), pa.field("b", pa.string())])
nsave("struct", pa.table({
    "id": pa.array(range(n), type=pa.int32()),
    "s": pa.array([maybe({"a": maybe(r.randint(0, 99)), "b": maybe("x%d" % r.randint(0, 9))}) for _ in range(n)], type=st_t),
    "s_all_null_fields": pa.array([{"a": None, "b": None} if i % 3 == 0 else (None if i % 3 == 1 else {"a": 1, "b": "q"}) for i in range(n)], type=st_t),
    "ss": pa.array([maybe({"c": maybe({"d": maybe(r.randint(0, 9)), "e": maybe(r.random())}), "f": maybe(r.randint(0, 9))}) for _ in range(n)],
                   type=pa.struct([pa.field("c", pa.struct([pa.field("d", pa.int64()), pa.field("e", pa.float64())])), pa.field("f", pa.int16())])),
    "req": pa.array([{"x": r.randint(0, 9), "y": {"z": r.randint(0, 9)}} for _ in range(n)],
                    type=pa.struct([pa.field("x", pa.int32(), nullable=False), pa.field("y", pa.struct([pa.field("z", pa.int32(), nullable=False)]), nullable=False)])),
    "mixed": pa.array([maybe({"x": r.randint(0, 9), "y": {"z": maybe(r.randint(0, 9))}}) for _ in range(n)],
                      type=pa.struct([pa.field("x", pa.int32(), nullable=False), pa.field("y", pa.struct([pa.field("z", pa.int32())]), nullable=False)])),
}))

inner = pa.struct([pa.field("a", pa.int32()), pa.field("b", lt(pa.string()))])
nsave("list-struct", pa.table({
    "id": pa.array(range(n), type=pa.int32()),
    "ls": pa.array([rand_list(lambda: maybe({"a": maybe(r.randint(0, 99)), "b": rand_list(lambda: "w%d" % r.randint(0, 9), maxlen=3)}), maxlen=4) for _ in range(n)], type=lt(inner)),
    "ls_req": pa.array([[{"a": r.randint(0, 99), "b": [r.choice(["u", "v"]) for _ in range(r.randint(0, 3))]} for _ in range(r.randint(0, 3))] for _ in range(n)],
                       type=lt(pa.struct([pa.field("a", pa.int32(), nullable=False), pa.field("b", lt(pa.string(), False), nullable=False)]), False)),
}))

mt = pa.map_(pa.string(), pa.int32())
nsave("map", pa.table({
    "id": pa.array(range(n), type=pa.int32()),
    "m": pa.array([None if r.random() < 0.2 else [("k%d" % j, maybe(r.randint(0, 99))) for j in range(r.randint(0, 4))] for _ in range(n)], type=mt),
    "m_numeric_keys": pa.array([None if r.random() < 0.2 else [(str(j * 3), r.randint(0, 9)) for j in range(r.randint(0, 4))] for _ in range(n)], type=mt),
    "m_dup": pa.array([[("a", 1), ("b", 2), ("a", 3)] if i % 5 == 0 else [("z", i)] for i in range(n)], type=mt),
    "m_int": pa.array([None if r.random() < 0.2 else [(j * 7 - 5, maybe("v%d" % j)) for j in range(r.randint(0, 4))] for _ in range(n)], type=pa.map_(pa.int32(), pa.string())),
    "m_seq": pa.array([[(j, "s%d" % j) for j in range(r.randint(0, 3))] for _ in range(n)], type=pa.map_(pa.int64(), pa.string())),
    "m_list_struct": pa.array([None if r.random() < 0.2 else [("k%d" % j, rand_list(lambda: {"x": maybe(r.randint(0, 9)), "y": maybe("t")}, maxlen=3)) for j in range(r.randint(0, 3))] for _ in range(n)],
                              type=pa.map_(pa.string(), lt(pa.struct([pa.field("x", pa.int32()), pa.field("y", pa.string())])))),
    "m_bool": pa.array([None if r.random() < 0.2 else [(j % 2 == 0, r.randint(0, 9)) for j in range(r.randint(0, 2))] for _ in range(n)], type=pa.map_(pa.bool_(), pa.int32())),
    "m_double": pa.array([None if r.random() < 0.2 else [(j * 0.5, "d") for j in range(r.randint(0, 3))] for _ in range(n)], type=pa.map_(pa.float64(), pa.string())),
    "m_date": pa.array([[(j * 1000, j) for j in range(r.randint(0, 3))] for _ in range(n)], type=pa.map_(pa.date32(), pa.int32())),
    "m_dec": pa.array([[(__import__("decimal").Decimal(j * 111).scaleb(-2), j) for j in range(r.randint(0, 3))] for _ in range(n)], type=pa.map_(pa.decimal128(9, 2), pa.int32())),
}))

# depth 6: struct<a: struct<b: list<struct<c: list<struct<d: int32, e: list<string>>>>>>>
d6 = pa.struct([pa.field("a", pa.struct([pa.field("b", lt(pa.struct([pa.field("c", lt(pa.struct([pa.field("d", pa.int32()), pa.field("e", lt(pa.string()))])))])))]))])
def gen_d6():
    return maybe({"a": maybe({"b": rand_list(lambda: maybe({"c": rand_list(lambda: maybe({"d": maybe(r.randint(0, 9)), "e": rand_list(lambda: "e%d" % r.randint(0, 3), maxlen=2)}), maxlen=3)}), maxlen=3)})}, 0.15)
nsave("deep", pa.table({"id": pa.array(range(n), type=pa.int32()), "d": pa.array([gen_d6() for _ in range(n)], type=d6)}))

# every list empty; one row of 100 000 elements; zero rows
nsave("empty-lists", pa.table({"l": pa.array([[] for _ in range(50)], type=lt(pa.int32())), "ls": pa.array([[] for _ in range(50)], type=lt(lt(pa.string())))}))
big_rows = [[], None, [i % 1000 for i in range(100000)], [1, None, 3], []]
save("nest-large-list", pa.table({"l": pa.array(big_rows, type=lt(pa.int32()))}), data_page_size=64 * 1024, write_batch_size=1000, compression="zstd")  # (the test knows the values: no expected trees)
save("nest-zero-rows", pa.table({"l": pa.array([], type=lt(pa.int32())), "s": pa.array([], type=st_t), "m": pa.array([], type=mt)}), nested=True)

# RLE booleans (data page v2)
save("rle-boolean", pa.table({"b": pa.array([i % 3 == 0 or i % 7 == 0 for i in range(300)]), "ob": pa.array([None if i % 4 == 0 else i % 5 == 0 for i in range(300)])}),
     use_dictionary=False, column_encoding={"b": "RLE", "ob": "RLE"}, data_page_version="2.0", compression="none")


# ---- test vectors for the unit tests, computed by independent Python code (oracle.py's formatters, int.from_bytes, the lz4 package) ----
import lz4.block
import lz4.frame

vr = random.Random(2026)
vectors = {"decimal": [], "date": [], "timestamp": [], "time": [], "lz4": [], "decimalCompare": []}
for length in range(1, 33):
    samples = [b"\x00" * length, b"\xff" * length, b"\x00" * (length - 1) + b"\x01", b"\xff" * (length - 1) + b"\xfe", b"\x80" + b"\x00" * (length - 1),
               b"\x7f" + b"\xff" * (length - 1), b"\x80" + b"\x00" * (length - 2) + b"\x01" if length > 1 else b"\x80"]
    samples += [bytes(vr.randint(0, 255) for _ in range(length)) for _ in range(4)]
    for raw in samples:
        n = int.from_bytes(raw, "big", signed=True)
        for scale in sorted({0, 2, min(length, 9)}):
            vectors["decimal"].append([raw.hex(), scale, oracle.fmt_decimal(n, scale)])
vectors["decimal"].append(["", 3, "0.000"])
# cross-check the Python formatter against Arrow's own string cast where Arrow can hold the value
for days in [0, 1, -1, 59, 60, 365, 366, 11016, 18262, 19000, -719162, -719528, 2932896, 2932897, -141427, -678941, -25508] + [vr.randint(-800000, 3000000) for _ in range(300)] + [INT32_MIN, INT32_MAX, INT32_MIN + 1, 146097, -146097, 146097 * 1000]:
    vectors["date"].append([days, oracle.fmt_date(days)])
inrange = [d for d, _ in vectors["date"] if -719162 <= d <= 2932896]
arrow_str = pa.array(inrange, type=pa.date32()).cast(pa.string()).to_pylist()
assert arrow_str == [oracle.fmt_date(d) for d in inrange], "the Python date formatter disagrees with Arrow's own cast"
for unit, per, dg in [("ms", 1000, 3), ("us", 10**6, 6), ("ns", 10**9, 9)]:
    vals = [0, 1, -1, 86399 * per, 86400 * per, -86400 * per, -86400 * per - 1, 1700000000 * per + 123, 2**63 - 1, -(2**63), 253402300799 * per, 253402300800 * per, -62135596800 * per, -62135596801 * per] + [vr.randint(-(2**62), 2**62) for _ in range(60)]
    vals = [v for v in vals if -(2**63) <= v < 2**63]
    for v in vals:
        vectors["timestamp"].append([unit, v, oracle.fmt_timestamp(v, per, dg, ""), oracle.fmt_timestamp(v, per, dg, "Z")])
    for v in [0, 1, 999, per - 1, per, 3661 * per + 1, 86399 * per + per - 1, 86400 * per, 100 * 3600 * per, -1, -3661 * per - 5]:
        vectors["time"].append([unit, v, oracle.fmt_time(v, per, dg)])
    ar = [v for v in vals if -(2**62) < v < 2**62 and unit == "us" and -62135596800 * per <= v < 253402300800 * per]
    if ar:
        cast = pa.array(ar, type=pa.timestamp("us")).cast(pa.string()).to_pylist()
        for v, c in zip(ar, cast):
            want = oracle.fmt_timestamp(v, per, dg, "")
            assert c.replace(" ", "T").rstrip("0").rstrip(".") == want.rstrip("0").rstrip("."), (v, c, want)
dec_cast_in = [int.from_bytes(bytes.fromhex(h), "big", signed=True) for h, sc, _ in vectors["decimal"] if len(h) <= 62 and sc == 2]
dec_arrow = pa.array([D(n).scaleb(-2) for n in dec_cast_in], type=pa.decimal256(76, 2)).cast(pa.string()).to_pylist()
dec_py = [e for h, sc, e in vectors["decimal"] if len(h) <= 62 and sc == 2]
assert dec_arrow == dec_py, "the Python decimal formatter disagrees with Arrow's own cast"
# decimal strings compared by value with Python's Decimal (the library's RangeCompare must agree)
def rand_dec():
    sign = r2.choice(["", "-", "-", ""])
    ip = r2.choice(["0", "00", "1", "12", "123456789012345678901234567890", str(r2.randint(0, 10**r2.randint(1, 25))), "007"])
    fp = r2.choice(["", "0", "00", "5", "50", "05", str(r2.randint(0, 10**r2.randint(1, 12))), "000000000000000000000001"])
    return sign + ip + ("." + fp if fp != "" else "")
r2 = random.Random(31)
for _ in range(600):
    a, b = rand_dec(), rand_dec()
    da, db = D(a), D(b)
    vectors["decimalCompare"].append([a, b, (da > db) - (da < db)])
lz_inputs = [b"", b"a", b"hello hello hello hello hello hello", bytes(range(256)) * 3, b"abc" * 5000, bytes(vr.randint(0, 255) for _ in range(3000)),
             (b"the quick brown fox jumps over the lazy dog " * 40) + bytes(vr.randint(0, 255) for _ in range(500)) + b"x" * 700]
for plain in lz_inputs:
    block = lz4.block.compress(plain, store_size=False, mode="high_compression") if plain else b"\x00"
    frame = lz4.frame.compress(plain)
    hadoop = len(plain).to_bytes(4, "big") + len(block).to_bytes(4, "big") + block
    vectors["lz4"].append({"plain": plain.hex(), "block": block.hex(), "hadoop": hadoop.hex(), "frame": frame.hex()})
with open(os.path.join(HERE, "vectors.json"), "w") as f:
    json.dump(vectors, f, separators=(",", ":"))
print("vectors.json: %d decimal, %d date, %d timestamp, %d time, %d lz4" % tuple(len(vectors[k]) for k in ["decimal", "date", "timestamp", "time", "lz4"]))


# ---- page indexes (offset index and column index), small pages, several row groups: for the row range and candidate range tests ----
import decimal as _dec
ri = random.Random(77)
N = 300
words = sorted("w%04d" % ri.randint(0, 9999) for _ in range(N))
idx_table = pa.table({
    "k": pa.array([i * 3 for i in range(N)], type=pa.int64()),                                        # sorted
    "u": pa.array([ri.randint(-1000, 1000) for _ in range(N)], type=pa.int32()),                      # unsorted
    "f": pa.array([float("nan") if i % 17 == 0 else ri.uniform(-50, 50) for i in range(N)], type=pa.float64()),
    "fs": pa.array([i * 0.5 for i in range(N)], type=pa.float32()),                                   # sorted floats
    "s": pa.array(words),                                                                             # sorted strings
    "su": pa.array(["%s-%d" % (ri.choice(["a", "b\xe9", "c"]), ri.randint(0, 99)) for _ in range(N)]),  # unsorted strings
    "d": pa.array([_dec.Decimal(ri.randint(-99999, 99999)).scaleb(-2) for _ in range(N)], type=pa.decimal128(7, 2)),
    "dt": pa.array([10000 + i * 7 for i in range(N)], type=pa.date32()),
    "ts": pa.array([1600000000000000 + i * 1000003 for i in range(N)], type=pa.timestamp("us", tz="UTC")),
    "n": pa.array([None if 100 <= i < 200 or ri.random() < 0.08 else i for i in range(N)], type=pa.int64()),  # row group 1 is all null: a null page
    "b": pa.array([i % 2 == 0 for i in range(N)]),
})
for version, tag in [("1.0", "v1"), ("2.0", "v2")]:
    for dictionary in [False, True]:
        pq.write_table(idx_table, os.path.join(OUT, "idx-flat-%s-%s.parquet" % (tag, "dict" if dictionary else "plain")), row_group_size=100, data_page_size=1,
                       write_batch_size=10, write_page_index=True, data_page_version=version, use_dictionary=dictionary, compression="snappy",
                       write_statistics=True)
nested_idx = pa.table({
    "id": pa.array(range(200), type=pa.int32()),
    "l": pa.array([None if i % 11 == 0 else [j for j in range(i % 5)] for i in range(200)], type=pa.list_(pa.field("element", pa.int32()))),
    "st": pa.array([None if i % 13 == 0 else {"a": i, "b": [str(i)] * (i % 3)} for i in range(200)],
                   type=pa.struct([pa.field("a", pa.int32()), pa.field("b", pa.list_(pa.field("element", pa.string())))])),
    "m": pa.array([[("k%d" % (i % 4), i)] * (i % 3) for i in range(200)], type=pa.map_(pa.string(), pa.int32())),
})
for version, tag in [("1.0", "v1"), ("2.0", "v2")]:
    pq.write_table(nested_idx, os.path.join(OUT, "idx-nested-%s.parquet" % tag), row_group_size=80, data_page_size=200, write_batch_size=8,
                   write_page_index=True, data_page_version=version, compression="zstd")
pq.write_table(idx_table, os.path.join(OUT, "idx-none.parquet"), row_group_size=100, data_page_size=1, write_batch_size=10, write_page_index=False)
print("page-index fixtures written")

# a tiny file for exhaustive (offset, limit) tests: 24 rows, 3 row groups of 8, about 3 rows per page
rs = random.Random(5)
small = pa.table({
    "k": pa.array(list(range(24)), type=pa.int64()),
    "s": pa.array([None if i % 5 == 0 else "s%d" % rs.randint(0, 9) for i in range(24)]),
    "l": pa.array([None if i % 7 == 0 else [j for j in range(i % 4)] for i in range(24)], type=pa.list_(pa.field("element", pa.int32()))),
    "st": pa.array([None if i % 6 == 0 else {"a": i, "m": ({"x": i} if i % 2 else None)} for i in range(24)],
                   type=pa.struct([pa.field("a", pa.int32()), pa.field("m", pa.struct([pa.field("x", pa.int32())]))])),
})
for version, tag in [("1.0", "v1"), ("2.0", "v2")]:
    pq.write_table(small, os.path.join(OUT, "idx-small-%s.parquet" % tag), row_group_size=8, data_page_size=24, write_batch_size=3, write_page_index=True,
                   data_page_version=version, compression="snappy")
pq.write_table(small, os.path.join(OUT, "idx-small-none.parquet"), row_group_size=8, data_page_size=24, write_batch_size=3, write_page_index=False, compression="snappy")
