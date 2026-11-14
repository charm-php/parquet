"""
Compares what charm/parquet read (the JSON written by dump.php) with what pyarrow reads from the same file.

    python tests/tools/oracle.py FILE DUMP.json

Prints one line: "PASS", "FAIL <first difference>" or "ORACLE-CANNOT-READ <pyarrow's error>".
With a dump that holds no columns it only checks whether pyarrow can read the file ("PASS" = it can).

The library returns PHYSICAL values; pyarrow applies logical types. So pyarrow's values are turned back into the stored form, per
the physical type that the Parquet schema (as pyarrow reports it) declares for the column:
  BOOLEAN    t / f
  INT32/INT64  the integer: signed ints as they are, unsigned ints (uint8..uint64) and dates, times, timestamps reinterpreted
             as the signed integer of the same width (zero-copy `view`) or, for 8 and 16 bit ints, cast to int32;
             decimals as their unscaled integer
  FLOAT/DOUBLE the IEEE bit pattern (big-endian hex) of the value, read from the Arrow buffer, so NaN payloads and -0.0 count
  BYTE_ARRAY the bytes (strings encoded as UTF-8); a DECIMAL stored as BYTE_ARRAY is compared as a number (the writer may have
             used any number of bytes), as the two's complement big-endian integer
  FIXED_LEN_BYTE_ARRAY the bytes; decimals as the unscaled integer in big-endian two's complement of the column's length;
             float16 from the 2 bytes of the Arrow buffer
  INT96      12 bytes: little-endian nanoseconds-of-day (8 bytes), then the little-endian Julian day (4 bytes). pyarrow is asked
             for timestamp[us] (ns overflows for dates far from 1970), so INT96 is compared at microsecond resolution: the
             library's nanoseconds-of-day are floor-divided by 1000 and compared with pyarrow's microseconds-of-day and the Julian
             day. Sub-microsecond digits are therefore not verified.

LOGICAL mode (the dump says "mode": "logical"): the library returns PHP values converted by the logical types. The expected values are
derived from pyarrow's arrays through their INTEGER storage (`view`, so nothing goes through Python datetime objects), formatted by
the independent Python code in this file (fmt_date, fmt_time, fmt_timestamp, fmt_decimal: a different algorithm from the PHP code)
and then put in the same canonical text as dump.php writes for a PHP value: int -> decimal digits; float -> the IEEE double bits
(big-endian hex; "nan" for any NaN); string -> hex of its bytes; bool t/f; a PHP array (INTERVAL) -> hex of "months,days,ms".
What pyarrow decides: which logical type a column has (its Arrow type), so a type that pyarrow reads as something else (an annotation
on the wrong physical type, for example) is compared as pyarrow reads it. Not verifiable here: INTERVAL (pyarrow has no mapping;
the corpus has no INTERVAL column), the nanosecond digits of INT96 (pyarrow is asked for microseconds), years outside what
Arrow can hold are still formatted from the integer value.
Nulls are None on both sides. Rows are compared across all row groups, in order.
"""
import json
import os
import struct
import sys

import pyarrow as pa
import pyarrow.parquet as pq


def unscaled(d, scale):
    t = d.as_tuple()
    n = int("".join(map(str, t.digits)) or "0")
    shift = t.exponent + scale
    if shift > 0:
        n *= 10 ** shift
    elif shift < 0:
        n //= 10 ** (-shift)
    return -n if t.sign else n


def leaf_array(table, path):
    arr = table.column(path[0])
    for part in path[1:]:
        chunks = []
        for ch in arr.chunks:
            if ch.type.get_field_index(part) < 0:
                raise KeyError("no field %s" % part)
            chunks.append(ch.flatten()[ch.type.get_field_index(part)])
        arr = pa.chunked_array(chunks, type=chunks[0].type) if chunks else arr
    return arr


def fixed_bytes(a, width):
    raw = a.buffers()[1].to_pybytes()
    valid = a.is_valid().to_pylist()
    return [raw[(a.offset + i) * width:(a.offset + i + 1) * width] if v else None for i, v in enumerate(valid)]


import datetime
import math

EPOCH = datetime.date(1970, 1, 1)


def fmt_date(days):
    """YYYY-MM-DD, proleptic Gregorian. Independent method: shift by whole 400-year cycles (146097 days) into the range Python's
    datetime handles, format there, and add the years back."""
    k = days // 146097
    d = EPOCH + datetime.timedelta(days=days - k * 146097)  # a date in 1970..2369
    year = d.year + 400 * k
    return ("-%04d" % -year if year < 0 else "%04d" % year) + "-%02d-%02d" % (d.month, d.day)


def fmt_frac(rem, per_sec, digits):
    secs, frac = divmod(rem, per_sec)
    minutes, s = divmod(secs, 60)
    h, m = divmod(minutes, 60)
    return "%02d:%02d:%02d.%0*d" % (h, m, s, digits, frac)


def fmt_time(v, per_sec, digits):
    return ("-" + fmt_frac(-v, per_sec, digits)) if v < 0 else fmt_frac(v, per_sec, digits)


def fmt_timestamp(v, per_sec, digits, suffix):
    days, rem = divmod(v, 86400 * per_sec)
    return fmt_date(days) + "T" + fmt_frac(rem, per_sec, digits) + suffix


def fmt_decimal(n, scale):
    digits = str(abs(n))
    if scale:
        digits = digits.rjust(scale + 1, "0")
        digits = digits[:-scale] + "." + digits[-scale:]
    return ("-" if n < 0 else "") + digits


def f64_canon(x):
    return "nan" if math.isnan(x) else struct.pack(">d", x).hex()


UNITS = {"ms": (1000, 3), "us": (10**6, 6), "ns": (10**9, 9), "s": (1, 0)}


def to_logical(arr, physical, type_length):
    """The expected canonical values for the library's LOGICAL mode (see the module docstring)."""
    out = []
    hx = lambda text: text.encode().hex()  # noqa: E731
    for a in arr.chunks:
        if pa.types.is_dictionary(a.type):
            a = a.dictionary_decode()
        is_uuid = str(a.type) == "extension<arrow.uuid>"
        if hasattr(a, "storage"):
            a = a.storage
        t = a.type
        valid = a.is_valid().to_pylist()
        if pa.types.is_null(t):
            out.extend([None] * len(a))
        elif pa.types.is_boolean(t):
            out.extend(None if v is None else ("t" if v else "f") for v in a.to_pylist())
        elif pa.types.is_integer(t):
            for v in a.to_pylist():
                out.append(None if v is None else (str(v) if v <= 2**63 - 1 else hx(str(v))))
        elif pa.types.is_float32(t):
            out.extend(None if v is None else f64_canon(struct.unpack("<f", struct.pack("<f", v))[0]) for v in a.to_pylist())
        elif pa.types.is_float64(t):
            out.extend(None if v is None else f64_canon(v) for v in a.to_pylist())
        elif pa.types.is_float16(t):
            out.extend(None if b is None else f64_canon(struct.unpack("<e", b)[0]) for b in fixed_bytes(a, 2))
        elif pa.types.is_date32(t):
            out.extend(None if v is None else hx(fmt_date(v)) for v in a.view(pa.int32()).to_pylist())
        elif pa.types.is_time(t):
            per, dg = UNITS[t.unit]
            ints = a.view(pa.int32() if pa.types.is_time32(t) else pa.int64()).to_pylist()
            out.extend(None if v is None else hx(fmt_time(v, per, dg)) for v in ints)
        elif pa.types.is_timestamp(t):
            per, dg = UNITS[t.unit]
            suffix = "" if (t.tz is None or physical == "INT96") else "Z"
            if physical == "INT96":
                dg = 9
            for v in a.view(pa.int64()).to_pylist():
                if v is None:
                    out.append(None)
                elif physical == "INT96":  # pyarrow gave microseconds: pad to nine digits (the comparison ignores the last three)
                    out.append(hx(fmt_timestamp(v * 1000, 10**9, 9, "")))
                else:
                    out.append(hx(fmt_timestamp(v, per, dg, suffix)))
        elif pa.types.is_decimal(t):
            out.extend(None if v is None else hx(fmt_decimal(unscaled(v, t.scale), t.scale)) for v in a.to_pylist())
        elif pa.types.is_string(t) or pa.types.is_large_string(t):
            out.extend(None if v is None else v.encode().hex() for v in a.to_pylist())
        elif is_uuid:
            for v in a.to_pylist():
                h = None if v is None else v.hex()
                out.append(None if h is None else hx("-".join([h[:8], h[8:12], h[12:16], h[16:20], h[20:]])))
        elif pa.types.is_binary(t) or pa.types.is_large_binary(t) or pa.types.is_fixed_size_binary(t):
            out.extend(None if v is None else v.hex() for v in a.to_pylist())
        else:
            raise ValueError("no logical mapping for arrow type " + str(t))
    return out


# ---- nested columns: pyarrow's nested arrays converted to the library's shapes, in the canonical text form ----
#
# A canonical tree is: None; a string (a leaf in the canonical text of to_physical / to_logical, floats as double bits or "nan");
# a JSON list (a list, a list of [key, value] pairs, or an associative array whose keys are exactly 0..n-1); or
# {"o": [[key, value], ...]} for an associative array (a struct, or a map): key is "i<int>" or "s<hex of the bytes>".
# What PHP does to array keys is mimicked: a numeric-string key becomes an int key, a repeated key keeps its last value (at its first
# position). A map is an associative array when the SCHEMA type of its key says so (physical INT32, INT64, BYTE_ARRAY or
# FIXED_LEN_BYTE_ARRAY whose value is an int or a string), else a list of [key, value] pairs; `pairs=True` forces pairs.

import re

INT_KEY = re.compile(r"^(0|-?[1-9][0-9]{0,18})$")


def php_key(v):
    """The PHP array key for an int, str or bytes value."""
    if isinstance(v, bytes):
        v = v.decode("latin1")
        raw = True
    else:
        raw = False
    if isinstance(v, int):
        return v
    if INT_KEY.match(v) and -(2**63) <= int(v) <= 2**63 - 1:
        return int(v)
    return v.encode("latin1" if raw else "utf-8")


def key_text(k):
    return "i%d" % k if isinstance(k, int) else "s" + k.hex()


class Skip(Exception):
    """A value the oracle cannot express (reported as ORACLE-CANNOT-READ)."""


def leaf_values(a, physical, length, mode):
    chunked = pa.chunked_array([a])
    if mode == "logical":
        return to_logical(chunked, physical, length)
    vals = to_physical(chunked, physical, length)
    out = []
    for v in vals:
        if isinstance(v, tuple):
            raise Skip("a physical %s value the oracle cannot reconstruct" % physical)
        if v is not None and physical == "FLOAT":
            v = f64_canon(struct.unpack(">f", bytes.fromhex(v))[0])
        elif v is not None and physical == "DOUBLE":
            v = f64_canon(struct.unpack(">d", bytes.fromhex(v))[0])
        out.append(v)
    return out


def key_values(a, physical, length, mode):
    """The PHP-visible key values of a map key array (ints, or text/bytes), or None when the key cannot be an array key."""
    if pa.types.is_dictionary(a.type):
        a = a.dictionary_decode()
    is_ext = hasattr(a, "storage")
    orig = a
    if is_ext:
        a = a.storage
    t = a.type
    if mode == "logical":
        if pa.types.is_float16(t) or pa.types.is_floating(t) or pa.types.is_boolean(t) or physical in ("INT96", "BOOLEAN", "FLOAT", "DOUBLE"):
            return None
        if pa.types.is_integer(t):
            return [None if v is None else (v if v <= 2**63 - 1 else str(v)) for v in a.to_pylist()]
        return [None if c is None else bytes.fromhex(c) for c in to_logical(pa.chunked_array([orig]), physical, length)]  # (orig: a uuid extension array keeps its type)
    if physical in ("INT32", "INT64"):
        return [None if c is None else int(c) for c in to_physical(pa.chunked_array([a]), physical, length)]
    if physical in ("BYTE_ARRAY", "FIXED_LEN_BYTE_ARRAY"):
        vals = to_physical(pa.chunked_array([a]), physical, length)
        if any(isinstance(v, tuple) for v in vals):
            raise Skip("a physical decimal in BYTE_ARRAY")
        return [None if c is None else bytes.fromhex(c) for c in vals]
    return None


# pyarrow reads a MAP whose value column is missing (a set, data/map_no_value.parquet) as a list of its keys; this library returns it as a
# map with null values (key => null), as decided. The oracle converts pyarrow's list to that form for these fields.
KEY_ONLY_MAPS = {("map_no_value.parquet", "my_map_no_v")}


def as_set(rows):
    if rows is None:
        return None
    return {"o": [[key_text(php_key(int(k))), None] for k in rows]} if rows else []


def conv(a, ctx):
    """A pyarrow Array to a list of canonical trees. ctx: {"sch": parquet schema, "leaf": next leaf ordinal, "mode", "pairs"}"""
    if pa.types.is_dictionary(a.type):
        a = a.dictionary_decode()
    t = a.type
    if pa.types.is_struct(t):
        valid = a.is_valid().to_pylist()
        kids = []
        for i in range(t.num_fields):
            kids.append((t.field(i).name, conv(a.field(i), ctx)))
        return [None if not valid[r] else {"o": [["s" + name.encode().hex(), col[r]] for name, col in kids]} for r in range(len(a))]
    if pa.types.is_map(t):
        offs = a.offsets.to_pylist()
        valid = a.is_valid().to_pylist()
        ordinal = ctx["leaf"]
        kc = ctx["sch"].column(ordinal)
        keys_arr = a.keys
        kv = None if ctx["pairs"] else key_values(keys_arr, kc.physical_type, kc.length if kc.physical_type == "FIXED_LEN_BYTE_ARRAY" else None, ctx["mode"])
        keys = conv(keys_arr, ctx)
        vals = conv(a.items, ctx)
        out = []
        for r in range(len(a)):
            if not valid[r]:
                out.append(None)
                continue
            lo, hi = offs[r], offs[r + 1]
            if kv is None:
                out.append([[keys[i], vals[i]] for i in range(lo, hi)])
            else:
                d = {}
                for i in range(lo, hi):
                    d[php_key(kv[i])] = vals[i]
                if list(d.keys()) == list(range(len(d))):
                    out.append(list(d.values()))
                else:
                    out.append({"o": [[key_text(k), v] for k, v in d.items()]})
        return out
    if pa.types.is_list(t) or pa.types.is_large_list(t) or pa.types.is_fixed_size_list(t):
        if pa.types.is_fixed_size_list(t):
            n = t.list_size
            offs = [i * n for i in range(len(a) + 1)]
        else:
            offs = a.offsets.to_pylist()
        valid = a.is_valid().to_pylist()
        vals = conv(a.values, ctx)
        return [None if not valid[r] else vals[offs[r]:offs[r + 1]] for r in range(len(a))]
    ordinal = ctx["leaf"]
    ctx["leaf"] += 1
    col = ctx["sch"].column(ordinal)
    return leaf_values(a, col.physical_type, col.length if col.physical_type == "FIXED_LEN_BYTE_ARRAY" else None, ctx["mode"])


def top_trees(table, pf, mode, pairs=False):
    """top-level field name -> list of canonical trees (one per row), for every field of the file."""
    sch = pf.schema
    out = {}
    names = [sch.column(i).path.split(".")[0] for i in range(len(sch))]
    for name in table.column_names:
        start = names.index(name)
        ctx = {"sch": sch, "leaf": start, "mode": mode, "pairs": pairs}
        rows = []
        for chunk in table.column(name).chunks:
            rows.extend(conv(chunk, ctx))
            ctx["leaf"] = start  # (each chunk walks the same leaves)
        out[name] = rows
    return out


def to_physical(arr, physical, type_length):
    """Return (list of canonical strings or None, optional list of 'numbers' for decimal comparison)."""
    out = []
    for a in arr.chunks:
        if pa.types.is_dictionary(a.type):
            a = a.dictionary_decode()
        if hasattr(a, "storage"):  # an extension type (json, uuid, ...): compare its storage
            a = a.storage
        t = a.type
        if pa.types.is_null(t):
            out.extend([None] * len(a))
            continue
        if physical == "BOOLEAN":
            out.extend(None if v is None else ("t" if v else "f") for v in a.to_pylist())
        elif physical in ("INT32", "INT64"):
            width = 32 if physical == "INT32" else 64
            if pa.types.is_decimal(t):
                vals = [None if v is None else unscaled(v, t.scale) for v in a.to_pylist()]
            else:
                bits = t.bit_width if hasattr(t, "bit_width") else width
                if bits == width:
                    a = a.view(pa.int32() if width == 32 else pa.int64())
                else:
                    a = a.cast(pa.int32() if width == 32 else pa.int64())
                vals = a.to_pylist()
            out.extend(None if v is None else str(v) for v in vals)
        elif physical == "FLOAT":
            b = fixed_bytes(a, 4)
            out.extend(None if v is None else v[::-1].hex() for v in b)
        elif physical == "DOUBLE":
            b = fixed_bytes(a, 8)
            out.extend(None if v is None else v[::-1].hex() for v in b)
        elif physical == "BYTE_ARRAY":
            if pa.types.is_decimal(t):
                out.extend(None if v is None else ("dec", unscaled(v, t.scale)) for v in a.to_pylist())
            else:
                if pa.types.is_string(t) or pa.types.is_large_string(t):
                    a = a.cast(pa.binary() if pa.types.is_string(t) else pa.large_binary())
                out.extend(None if v is None else v.hex() for v in a.to_pylist())
        elif physical == "FIXED_LEN_BYTE_ARRAY":
            if pa.types.is_decimal(t):
                out.extend(None if v is None else unscaled(v, t.scale).to_bytes(type_length, "big", signed=True).hex() for v in a.to_pylist())
            elif pa.types.is_float16(t):
                out.extend(None if v is None else v.hex() for v in fixed_bytes(a, 2))
            else:
                out.extend(None if v is None else v.hex() for v in a.to_pylist())
        elif physical == "INT96":
            a = a.view(pa.int64()) if pa.types.is_timestamp(t) else a  # (microseconds: see the docstring)
            for v in a.to_pylist():
                if v is None:
                    out.append(None)
                else:
                    days, micros = divmod(v, 86400 * 10**6)
                    out.append(("int96us", micros, days + 2440588))
        else:
            raise ValueError("unknown physical type " + str(physical))
    return out


def main():
    path, dump_path = sys.argv[1], sys.argv[2]
    dump = json.load(open(dump_path))
    dump["columns"] = dump.get("columns") or {}  # (PHP writes an empty array as a JSON list)
    dump["trees"] = dump.get("trees") or {}
    try:
        pf = pq.ParquetFile(path, coerce_int96_timestamp_unit="us")
        table = pf.read()
        sch = pf.schema
    except Exception as e:  # noqa: BLE001 (any pyarrow failure means the oracle cannot read it)
        print("ORACLE-CANNOT-READ %s: %s" % (type(e).__name__, str(e).replace("\n", " ")[:200]))
        return
    if not dump["columns"] and not dump.get("trees"):
        print("PASS")
        return
    if dump.get("rows") is not None and dump["rows"] != table.num_rows and path.split("/")[-1] not in ("empty_data_pages_v1.parquet", "empty_data_pages_v2.parquet"):
        print("FAIL num_rows: library %s, pyarrow %s" % (dump["rows"], table.num_rows))
        return
    # pyarrow 26 reads 0 rows from these two files; their documentation (data/empty_data_pages.md) says the column holds 1, 2, 3.
    name = path.replace("\\", "/").split("/")[-1]
    overrides = {"empty_data_pages_v1.parquet": ["1", "2", "3"], "empty_data_pages_v2.parquet": ["1", "2", "3"]}
    if name in overrides:
        got = dump["columns"]["int64"]["values"]
        print("PASS" if got == overrides[name] else "FAIL empty_data_pages: library %r, documented %r" % (got, overrides[name]))
        return
    by_path = {sch.column(i).path: sch.column(i) for i in range(len(sch))}
    nvalues = 0
    for dotted, col in dump["columns"].items():
        try:
            info = by_path[dotted]
            arr = leaf_array(table, dotted.split("."))
            if dump.get("mode") == "logical":
                expected = to_logical(arr, info.physical_type, col.get("typeLength"))
            else:
                expected = to_physical(arr, info.physical_type, col.get("typeLength"))
        except Exception as e:  # noqa: BLE001
            print("ORACLE-CANNOT-READ column %s: %s: %s" % (dotted, type(e).__name__, str(e).replace("\n", " ")[:200]))
            return
        got = col["values"]
        nvalues += len(got)
        if len(got) != len(expected):
            print("FAIL column %s: %d values, pyarrow %d" % (dotted, len(got), len(expected)))
            return
        for i, (g, e) in enumerate(zip(got, expected)):
            if dump.get("mode") == "logical" and info.physical_type == "INT96" and isinstance(g, str) and isinstance(e, str):
                g_full = g
                g, e = g[:-6], e[:-6]  # hex of the text: drop the last three characters (sub-microsecond digits)
                year = bytes.fromhex(g_full).decode().split("-")[0] if not g_full.startswith("2d") else "-1"
                if g != e and (g_full.startswith("2d") or len(year) > 4):
                    print("ORACLE-CANNOT-READ column %s row %d: INT96 year outside 0001-9999 (pyarrow's microsecond conversion overflows)" % (dotted, i))
                    return
            if isinstance(e, tuple) and e[0] == "dec":  # a decimal stored as BYTE_ARRAY: compare as a number
                g = None if g is None else ("dec", int.from_bytes(bytes.fromhex(g), "big", signed=True))
            elif isinstance(e, tuple):  # INT96: pyarrow's microsecond resolution
                if g is not None:
                    nanos, day = struct.unpack("<qI", bytes.fromhex(g))
                    g = ("int96us", nanos // 1000, day)
            if g != e and isinstance(g, tuple) and g[0] == "int96us" and not (0 <= g[1] < 86400 * 10**6 and 0 <= g[2] < 5 * 10**6):
                # an INT96 whose nanoseconds-of-day or Julian day are out of range: pyarrow's microsecond conversion overflows
                print("ORACLE-CANNOT-READ column %s row %d: INT96 value outside what pyarrow can represent" % (dotted, i))
                return
            if g != e:
                print("FAIL column %s row %d: library %r, pyarrow %r" % (dotted, i, g if not isinstance(g, str) else g[:80], e if not isinstance(e, str) else e[:80]))
                return
    ntrees = 0
    if dump.get("trees"):
        try:
            expected_trees = top_trees(table, pf, dump.get("mode", "physical"))
        except Skip as e:
            print("ORACLE-CANNOT-READ nested: %s" % e)
            return
        except Exception as e:  # noqa: BLE001
            print("ORACLE-CANNOT-READ nested: %s: %s" % (type(e).__name__, str(e).replace("\n", " ")[:200]))
            return
        for name, rows in dump["trees"].items():
            want = expected_trees.get(name)
            if want is None:
                print("ORACLE-CANNOT-READ nested: pyarrow has no top-level field %s" % name)
                return
            if len(rows) != len(want):
                print("FAIL field %s: %d rows, pyarrow %d" % (name, len(rows), len(want)))
                return
            if (os.path.basename(path), name) in KEY_ONLY_MAPS:
                want = [as_set(w) for w in want]
            for i, (g, e) in enumerate(zip(rows, want)):
                if g != e:
                    print("FAIL field %s row %d: library %s, pyarrow %s" % (name, i, json.dumps(g)[:160], json.dumps(e)[:160]))
                    return
            ntrees += 1
    print("PASS %d columns, %d values compared%s" % (len(dump["columns"]), nvalues, (", %d nested fields" % ntrees) if ntrees else ""))


if __name__ == "__main__":
    main()
