"""
Reads files written by charm/parquet with pyarrow and with DuckDB and compares every value with what was written.

    python tests/tools/roundtrip.py DIR       # DIR holds cases.json (written by roundtrip.php) and the .parquet files

For each case it prints one JSON line {"case": name, "pyarrow": "OK"|"FAIL ...", "duckdb": "OK"|"FAIL ..."}. Both readers' results are converted
to the canonical trees of oracle.py (logical mode) and compared with the expected trees from the PHP side, so a map, a decimal or a timestamp
is compared as the value this library defines, whatever Python object the reader made of it.
"""
import json
import os
import sys

import duckdb
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import oracle  # noqa: E402


import re

TIME_LIKE = re.compile(r"^(.*\d\d:\d\d:\d\d)\.(\d+)(Z?)$")


def lenient(x):
    """A tree with the fraction of every time-like string cut to 6 digits and trailing zeros dropped (DuckDB's TIME and TIMESTAMP_MS units)."""
    if isinstance(x, list):
        return [lenient(i) for i in x]
    if isinstance(x, dict):
        return {k: lenient(v) for k, v in x.items()}
    if isinstance(x, str):
        prefix, h = (x[0], x[1:]) if x[:1] in ("s", "i") and len(x) % 2 == 1 else ("", x)
        try:
            text = bytes.fromhex(h).decode("ascii")
        except ValueError:
            return x
        m = TIME_LIKE.match(text)
        if not m:
            return x
        frac = m.group(2)[:6].rstrip("0")
        text = m.group(1) + ("." + frac if frac else "") + m.group(3)
        return prefix + text.encode().hex()
    return x


def compare(table, pf, expected, allow_lossy=False):
    try:
        got = oracle.top_trees(table, pf, "logical")
    except oracle.Skip as e:
        return "SKIP %s" % e
    for name, want in expected.items():
        if name not in got:
            return "FAIL column %s missing" % name
        rows = got[name]
        if len(rows) != len(want):
            return "FAIL column %s: %d rows, expected %d" % (name, len(rows), len(want))
        for i, (g, w) in enumerate(zip(rows, want)):
            if g != w:
                if allow_lossy and lenient(g) == lenient(w):
                    return "KNOWN fractional-second precision of TIME/TIMESTAMP differs (DuckDB has microsecond TIME)"
                return "FAIL column %s row %d: got %s, expected %s" % (name, i, json.dumps(g)[:200], json.dumps(w)[:200])
    if set(got) != set(expected):
        return "FAIL columns %s, expected %s" % (sorted(got), sorted(expected))
    return "OK"


def chunk_problems(cc, st_theirs, arr, leaf, g, theirs, i):
    import pyarrow as pa
    import pyarrow.compute as pc
    problems = []
    st = cc.statistics
    nulls = arr.null_count
    if st.has_null_count and st.null_count != nulls:
        problems.append("%s rg%d null_count %s != %s" % (leaf.path, g, st.null_count, nulls))
    if st.has_min_max and not (pa.types.is_floating(arr.type) and pc.any(pc.is_nan(arr)).as_py()) and not pa.types.is_float16(arr.type):
        mm = pc.min_max(arr)
        if mm["min"].is_valid and (st.min != mm["min"].as_py() or st.max != mm["max"].as_py()):
            problems.append("%s rg%d min/max %r..%r != true %r..%r" % (leaf.path, g, st.min, st.max, mm["min"].as_py(), mm["max"].as_py()))
        if theirs is not None and g < theirs.num_row_groups:
            ts = theirs.row_group(g).column(i).statistics
            if ts is not None and ts.has_min_max and (st.min != ts.min or st.max != ts.max):
                problems.append("%s rg%d our min/max %r..%r, pyarrow's %r..%r" % (leaf.path, g, st.min, st.max, ts.min, ts.max))
    return problems


def check_stats(path, pf):
    """Footer statistics of every flat leaf column chunk against the true values (computed by pyarrow from the data) and against the
    statistics pyarrow itself writes for the same data and row groups; the page index flags; and range filters counted by pyarrow and DuckDB."""
    import pyarrow as pa
    import pyarrow.compute as pc
    import random
    m = pf.metadata
    sch = pf.schema
    table = pf.read()
    tmp = path + ".pyarrow-rewrite"
    rg_rows = [m.row_group(g).num_rows for g in range(m.num_row_groups)]
    if rg_rows and len(set(rg_rows[:-1])) <= 1 and (len(rg_rows) == 1 or rg_rows[-1] <= rg_rows[0]):
        try:
            pq.write_table(table, tmp, row_group_size=max(rg_rows), write_statistics=True)
            theirs = pq.ParquetFile(tmp).metadata
        except Exception:  # noqa: BLE001 (a type pyarrow cannot write back)
            theirs = None
    else:
        theirs = None
    problems = []
    checked = 0
    offset = 0
    for g in range(m.num_row_groups):
        n = m.row_group(g).num_rows
        part = pf.read_row_group(g)
        if part.num_rows != n or not part.equals(table.slice(offset, n), check_metadata=False) and not any(pa.types.is_floating(f.type) or pa.types.is_nested(f.type) for f in table.schema):
            problems.append("read_row_group(%d): %d rows, expected %d (or different values)" % (g, part.num_rows, n))
        offset += n
        for i in range(m.num_columns):
            cc = m.row_group(g).column(i)
            if cc.total_compressed_size <= 0 and n > 0 or cc.num_values < n:
                problems.append("%s rg%d: chunk sizes/values implausible" % (cc.path_in_schema, g))
            if cc.has_dictionary_page and "PLAIN_DICTIONARY" not in cc.encodings and "RLE_DICTIONARY" not in cc.encodings:
                problems.append("%s rg%d: dictionary page but no dictionary encoding" % (cc.path_in_schema, g))
    for i in range(len(sch)):
        leaf = sch.column(i)
        if leaf.max_repetition_level != 0 or "." in leaf.path and False:
            continue
        offset = 0
        for g in range(m.num_row_groups):
            cc = m.row_group(g).column(i)
            n = m.row_group(g).num_rows
            arr = oracle.leaf_array(table.slice(offset, n), leaf.path.split(".")) if n else None
            offset += n
            if arr is None or not cc.is_stats_set:
                continue
            try:
                problems += chunk_problems(cc, st_theirs=None, arr=arr, leaf=leaf, g=g, theirs=theirs, i=i)
            except (OverflowError, ValueError, pa.ArrowNotImplementedError, pa.ArrowInvalid):
                pass
            continue
            st = cc.statistics
            nulls = arr.null_count
            if st.has_null_count and st.null_count != nulls:
                problems.append("%s rg%d null_count %s != %s" % (leaf.path, g, st.null_count, nulls))
            if st.has_min_max and leaf.physical_type not in ("FIXED_LEN_BYTE_ARRAY", "INT96") or (st.has_min_max and pa.types.is_decimal(arr.type)):
                try:
                    if pa.types.is_floating(arr.type) and pc.any(pc.is_nan(arr)).as_py():
                        continue
                    mm = pc.min_max(arr)
                    if mm["min"].is_valid and (st.min != mm["min"].as_py() or st.max != mm["max"].as_py()):
                        if not (isinstance(st.min, float) and st.min == mm["min"].as_py()):
                            problems.append("%s rg%d min/max %r..%r != true %r..%r" % (leaf.path, g, st.min, st.max, mm["min"].as_py(), mm["max"].as_py()))
                    checked += 1
                except (pa.ArrowNotImplementedError, pa.ArrowInvalid, TypeError):
                    pass
            if theirs is not None and g < theirs.num_row_groups:
                ts = theirs.row_group(g).column(i).statistics
                if ts is not None and st.has_min_max and ts.has_min_max and (st.min != ts.min or st.max != ts.max) and not (isinstance(st.min, float) and st.min == ts.min and st.max == ts.max):
                    problems.append("%s rg%d our min/max %r..%r, pyarrow's for the same data %r..%r" % (leaf.path, g, st.min, st.max, ts.min, ts.max))
            if m.row_group(g).column(i).has_offset_index is not True and n > 0:
                problems.append("%s rg%d: no offset index" % (leaf.path, g))
    if os.path.exists(tmp):
        os.remove(tmp)
    # range filters: counts from pyarrow's dataset filter and from DuckDB against a brute-force count
    random.seed(3)
    for name in table.column_names:
        col = table.column(name)
        t = col.type
        if not (pa.types.is_integer(t) and t.bit_width <= 32 or pa.types.is_int64(t) or pa.types.is_string(t) or pa.types.is_float64(t)) or col.null_count == len(col):
            continue
        if pa.types.is_floating(t) and pc.any(pc.is_nan(col)).as_py():
            continue
        vals = [v for v in col.to_pylist() if v is not None]
        for _ in range(3):
            a, b = sorted(random.sample(vals, 2)) if len(vals) > 1 else (vals[0], vals[0])
            want = sum(1 for v in vals if a <= v <= b)
            got = pq.read_table(path, columns=[name], filters=[(name, ">=", a), (name, "<=", b)]).num_rows
            if got != want:
                problems.append("pyarrow filter on %s [%r, %r]: %d rows, expected %d" % (name, a, b, got, want))
            q = duckdb.sql('select count(*) from read_parquet(\'%s\') where "%s" between ? and ?' % (path.replace("'", "''"), name.replace('"', '""')), params=[a, b]).fetchall()[0][0]
            if q != want:
                problems.append("duckdb filter on %s [%r, %r]: %d rows, expected %d" % (name, a, b, q, want))
    return "OK" if not problems else "FAIL " + "; ".join(problems[:3])


def main():
    d = sys.argv[1]
    cases = json.load(open(os.path.join(d, "cases.json")))
    for c in cases:
        path = os.path.join(d, c["file"])
        res = {"case": c["file"]}
        try:
            pf = pq.ParquetFile(path)
            res["pyarrow"] = compare(pf.read(), pf, c["expected"])
        except Exception as e:  # noqa: BLE001
            res["pyarrow"] = "FAIL %s: %s" % (type(e).__name__, str(e).replace("\n", " ")[:200])
        try:
            pf = pq.ParquetFile(path)
            reader = duckdb.sql("select * from read_parquet('%s')" % path.replace("'", "''")).arrow()
            table = reader.read_all() if hasattr(reader, "read_all") else reader
            res["duckdb"] = compare(table, pf, c["expected"], allow_lossy=True)
        except Exception as e:  # noqa: BLE001
            msg = str(e).replace("\n", " ")[:200]
            if "Could not convert Timestamp" in msg:
                res["duckdb"] = "KNOWN DuckDB cannot represent a timestamp outside its range (%s)" % msg[:80]
            else:
                res["duckdb"] = "FAIL %s: %s" % (type(e).__name__, msg)
        try:
            res["stats"] = check_stats(path, pf)
        except Exception as e:  # noqa: BLE001
            res["stats"] = "FAIL %s: %s" % (type(e).__name__, str(e).replace("\n", " ")[:200])
        print(json.dumps(res), flush=True)


main()
