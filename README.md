# charm/parquet

A pure-PHP Apache Parquet reader and writer (PHP 8.2+, 64-bit, no extension needed). It reads flat and nested columns (structs, lists, maps),
converts logical types to exact PHP values, and works on a **range function**: the library does no I/O of its own, so the same code
reads a local file, a string, or an object store over HTTP range requests, and fetches only the pages a query needs. The writer turns rows into the bytes of a file and does no I/O either.

```
composer require charm/parquet
```

Install note: the runtime dependencies are `charm/zstd` and `charm/brotli`, pure-PHP decoders for ZSTD and BROTLI pages. Until they are on Packagist, `composer.json` lists their GitHub repositories as `vcs` repositories; Composer reads that entry only in this package's own checkout, so a project that requires charm/parquet needs the two packages to be on Packagist (or the same two `vcs` entries in its own `composer.json`). While the packages are release candidates, a project needs `"minimum-stability": "RC"` (or `@RC` flags on all three). `ext-zlib` (GZIP pages) is optional (`suggest`); without it the reader throws `UnsupportedException` for GZIP.
`ext-brotli` is optional too: BROTLI pages are read with it when it is loaded (faster) and with the pure-PHP charm/brotli otherwise. 32-bit PHP is not supported (`php-64bit` is required, and the
Reader refuses to start otherwise).

Source: https://github.com/charm-php/parquet

## Usage

<!-- example: reader -->
```php
use Charm\Parquet\{MapMode, Reader, ValueMode};

$reader = Reader::fromFile('events.parquet');            // Reader::fromString($bytes) works the same
echo $reader->rowCount(), ' rows in ', $reader->rowGroupCount(), " row groups\n";

// Rows: one row group in memory at a time. A row is [top-level field name => value].
foreach ($reader->rows() as $row) {
    // ['id' => 17, 'user' => ['name' => 'Ann', 'tags' => ['a', 'b']], 'at' => '2024-05-01T12:00:00.000000Z']
}

// Only some columns (a path may name a leaf or a group: 'user' selects every field of the struct, 'user.name' only that one)
foreach ($reader->rows(['id', 'user.name']) as $row) {
    // ['id' => 17, 'user' => ['name' => 'Ann']]
}

// A row range: exactly rows 1000 .. 1049 of the file. Row groups and pages that hold none of them are not read.
foreach ($reader->rows(null, offset: 1000, limit: 50) as $row) {
}

// Columns of one row group, one entry per row
$ids = $reader->readColumn(0, 'id');                      // list<int|null>
$cols = $reader->readRowGroup(0, ['id', 'user'], offset: 10, limit: 5);

// Nested data: a leaf path gives the projected shape (list levels kept, wrapper structs dropped)
$reader->readColumn(0, 'user.tags.list.element');          // per row: a list of tags, or null

// Statistics and candidate ranges: which rows could hold ids between 1000 and 2000?
$ranges = $reader->candidateRanges('id', 1000, 2000);      // [[first row, row count], ...], conservative
foreach ($reader->rows(['id', 'user'], ranges: $ranges) as $row) {
    if ($row['id'] >= 1000 && $row['id'] <= 2000) { /* the caller filters */ }
}

// Inspecting the file
$meta = $reader->metadata();                               // version, createdBy, keyValueMetadata, rowGroups, schema
foreach ($reader->schema()->leaves as $leaf) {
    echo $leaf->dottedPath, ' ', $leaf->physicalType->name, ' def ', $leaf->maxDefinitionLevel, ' rep ', $leaf->maxRepetitionLevel, "\n";
}
$stats = $reader->statistics(0, 'id');                     // min, max, nullCount, distinctCount, isMinExact, isMaxExact
$index = $reader->offsetIndex(0, 'id');                    // pages: offset, size, first row (null: the file has none)
$ci = $reader->columnIndex(0, 'id');                       // per-page min/max/null counts (null: none)

// Physical values (stored representation) instead of logical ones; maps always as [key, value] pairs
$raw = Reader::fromFile('events.parquet', values: ValueMode::Physical, mapMode: MapMode::Pairs);

// One column chunk as the stored levels and values
$shredded = $reader->readShredded(0, 'user.tags.list.element');   // ->values, ->definitionLevels, ->repetitionLevels
```

### Over HTTP range requests

The Reader asks for exactly the byte ranges it needs: the last 64 KiB of the file (the footer; a second request only if the footer is
larger), the page index of a column when a row range or `candidateRanges()` needs it (one request serves the indexes of a row group
when they are adjacent), and the pages of the wanted rows (adjacent pages in one request). Any function `(int $offset, int $length): string`
that returns exactly the bytes asked for will do. With PHP streams:

<!-- example: http -->
```php
use Charm\Parquet\Reader;

function parquetOverHttp(string $url): Reader
{
    $head = get_headers($url, true, stream_context_create(['http' => ['method' => 'HEAD']]));
    $size = (int) $head['Content-Length'];
    $read = static function (int $offset, int $length) use ($url): string {
        $ctx = stream_context_create(['http' => ['header' => sprintf("Range: bytes=%d-%d\r\n", $offset, $offset + $length - 1), 'ignore_errors' => true]]);
        $body = file_get_contents($url, false, $ctx);
        $status = $http_response_header[0] ?? '';
        if (str_contains($status, ' 206 ')) {
            return $body;                                  // the server honoured the range
        }
        if (str_contains($status, ' 200 ')) {
            return substr($body, $offset, $length);        // it ignored it and sent the whole file: correct, but slow (see below)
        }
        throw new RuntimeException("Unexpected response: $status");
    };
    return new Reader($read, $size);
}
```

A server that ignores `Range` (it answers `200` with the whole body) still gives correct results with this function, but every
call downloads the whole file again. Check `Accept-Ranges: bytes` in the `HEAD` response first, and for a server without ranges
download the file once and use `Reader::fromString()`. The tests drive the Reader with a fake transport that records the requested
ranges (`tests/PageIndexTest.php`); the PHP-streams function above is not exercised by the tests (no network in the suite).

## API

```php
namespace Charm\Parquet;

final class Reader
{
    public function __construct(\Closure $read, int $size, ValueMode $values = ValueMode::Logical, int $maxPageSize = 1 << 30,
                                int $maxChunkValues = 1 << 25, MapMode $mapMode = MapMode::Auto);
    public static function fromFile(string $path, ValueMode $values = ..., int $maxPageSize = ..., int $maxChunkValues = ..., MapMode $mapMode = ...): self;
    public static function fromString(string $bytes, ...same): self;

    public function metadata(): FileMetadata;
    public function schema(): Schema;
    public function numRows(): int;        // the footer's num_rows (some writers get it wrong)
    public function rowCount(): int;       // the sum over the row groups
    public function rowGroupCount(): int;
    public function readCalls(): int;      // range calls made so far

    public function rows(?array $columns = null, int $offset = 0, ?int $limit = null, ?array $ranges = null): \Generator;
    public function readColumn(int $rowGroup, string $path, int $offset = 0, ?int $limit = null): array;
    public function readRowGroup(int $rowGroup, ?array $paths = null, int $offset = 0, ?int $limit = null): array;
    public function readShredded(int $rowGroup, string $leafPath): ShreddedColumn;

    public function statistics(int $rowGroup, string $leafPath): ?ColumnStatistics;
    public function offsetIndex(int $rowGroup, string $leafPath): ?OffsetIndex;
    public function columnIndex(int $rowGroup, string $leafPath): ?ColumnIndex;
    public function candidateRanges(string $leafPath, mixed $min = null, mixed $max = null, bool $orNull = false): array;
}

enum ValueMode { case Physical; case Logical; }       // Logical is the default
enum MapMode { case Auto; case Pairs; }

class ParquetException extends \UnexpectedValueException {}  // the file is corrupt, truncated or violates the format (not final)
final class UnsupportedException extends ParquetException {} // valid, but not implemented: the message names it
```

Value objects (all `final readonly`): `FileMetadata`, `Schema` (`root`, `leaves`, `leaf($dotted)`, `node($dotted)`), `SchemaNode`, `LogicalType`,
`RowGroup`, `ColumnChunk`, `Statistics` (raw bytes), `ColumnStatistics` (decoded), `ShreddedColumn`, `OffsetIndex`, `PageLocation`,
`ColumnIndex`. Enums: `PhysicalType`, `Repetition`, `Codec`, `Encoding`, `PageType`, `ConvertedType`, `LogicalKind`, `TimeUnit`, `BoundaryOrder`.

* **Paths** are the real schema names joined by dots (`a.list.element`, `m.key_value.key`), as `SchemaNode::$dottedPath` has them.
* **`rows($columns, $offset, $limit, $ranges)`** yields exactly the rows `[offset, offset + limit)` of the file, further restricted to
  `$ranges` (a list of `[first row, row count]`, in any order, overlapping allowed) when given. Row groups with no wanted row are never
  read. Inside a row group, when a column has an offset index, only the pages that hold wanted rows (and the dictionary page) are read, with
  adjacent pages in one range call; without an offset index the chunk is read and sliced. A negative offset or limit, or a malformed range,
  is a `\ValueError`.
* **`readColumn` / `readRowGroup`** take an offset and limit relative to the row group. A leaf path gives its projected shape; a group path the assembled value.
* **`candidateRanges($leafPath, $min, $max, $orNull)`** returns `[first row, row count]` ranges in file order (adjacent ones merged) that
  could hold a value in `[$min, $max]` (either bound may be `null`: open) or, with `$orNull`, a null. It uses the row group statistics and, when the file has
  them, the column index and offset index; it never reads data pages and does no per-value filtering. It is conservative: a row that could
  match is never left out. Missing statistics, a statistic that the footer flags inexact (`is_min_value_exact` / `is_max_value_exact` false), NaN, deprecated
  min/max of types with an unreliable old ordering, values of a kind that cannot be compared safely (an int bound on a string column), a date or timestamp
  string whose year has a sign or more than four digits, and INT96 / INTERVAL columns all mean "keep". Bounds are in the Reader's value mode:
  ints for integer columns, `"YYYY-MM-DD"` for a DATE in logical mode, a decimal string for a DECIMAL, bytes (compared unsigned) for strings.
  In physical mode a DECIMAL stored in bytes is compared as the signed big-endian number it is.
* **`offsetIndex` / `columnIndex`** return `null` when the file has none for the column. The index is fetched on first use and checked against the chunk
  (page locations inside the chunk, in order, not overlapping; first row indexes starting at 0 and not decreasing, strictly increasing for a column without
  repetition). pyarrow writes v1 pages of repeated columns that continue the previous page's last row with the same first row index; that is accepted.

## Value shapes

### Rows, structs, lists, maps

`rows()` yields rows keyed by **top-level field name**; every value is assembled:

* a **struct** is an associative array `field => value`; a null struct is `null`, a struct whose fields are all null is an array of nulls;
* a **list** (the 3-level LIST form, the legacy 2-level and 1-level forms of the format's backward-compatibility rules, and any unannotated
  `repeated` field) is a PHP list; null list `null`, empty list `[]`, a list of nulls `[null, null]`. An unannotated `repeated` primitive is a list
  of values, an unannotated `repeated` group is a list of structs;
* a **map** is decided by the schema type of its key, never by the values, so a column always has one shape: when the key is stored as
  INT32, INT64, BYTE_ARRAY or FIXED_LEN_BYTE_ARRAY and its value in the current `ValueMode` is an int or a string, an associative array
  `key => value` in file order; any other key type (BOOLEAN, FLOAT, DOUBLE, INT96, FLOAT16, INTERVAL, a group) gives a list of `[key, value]` pairs.
  `MapMode::Pairs` gives pairs for every map, the lossless form. A null map is `null`, an empty map `[]`; a map without a value column (a set) has `null` values.
  **Two PHP facts about array keys:** a numeric-string key such as `"42"` becomes the int key `42`, and a key that occurs twice keeps its last value
  (at the position of the first). Use `MapMode::Pairs` when that matters.

Leaves inside nesting get the same logical conversion as flat ones. VARIANT, GEOMETRY and the like are whatever their physical structure assembles to
(a struct of binaries), uninterpreted.

Selection: a path may name a leaf or a group; a group selects every leaf below it. Some leaves of a struct give a partial struct (only those fields); only the key
leaf of a map gives `key => null`. `readColumn($rowGroup, $path)` gives one entry per row: for a group path the assembled value, for a leaf path its projected shape (the
list levels on the way are kept, wrapper structs are dropped: for `a: list<struct<b, c>>` the path `a.list.element.b` gives, per row, a list of b values, with
null where the list or the struct is null).

### Physical values (`ValueMode::Physical`)

| Physical type | PHP value |
|---|---|
| BOOLEAN | `bool` |
| INT32, INT64 | `int` (64-bit PHP: the full range) |
| FLOAT, DOUBLE | `float` (a FLOAT is exact as a double; NaN payloads are not preserved, `-0.0` is) |
| BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY | the raw bytes as a `string`, not validated |
| INT96 | the 12 raw bytes |
| missing | `null` |

### Logical values (`ValueMode::Logical`, the default)

| Annotation (logical type, else the legacy converted type) | Physical | PHP value |
|---|---|---|
| signed integers (INT_8 ... INT_64, Integer(w, signed)) | INT32, INT64 | int |
| UINT_8, UINT_16, UINT_32 | INT32 | int |
| UINT_64 | INT64 | int when it fits in a PHP int, otherwise a decimal string |
| DECIMAL | INT32, INT64, FIXED_LEN_BYTE_ARRAY, BYTE_ARRAY | exact decimal string with exactly `scale` fractional digits (`"-123.45"`, `"0.05"`, `"123"`), any precision |
| DATE | INT32 | `"YYYY-MM-DD"` |
| TIME | INT32, INT64 | `"HH:MM:SS"` + 3, 6 or 9 fractional digits by unit |
| TIMESTAMP, TIMESTAMP_MILLIS/MICROS | INT64 | `"YYYY-MM-DDTHH:MM:SS"` + 3, 6 or 9 digits by unit, `Z` appended when isAdjustedToUTC (legacy timestamps are UTC) |
| (INT96 is always a timestamp) | INT96 | `"YYYY-MM-DDTHH:MM:SS.nnnnnnnnn"` (9 digits, no suffix) |
| STRING, ENUM, JSON, BSON | BYTE_ARRAY | the bytes, not validated |
| UUID | FIXED_LEN_BYTE_ARRAY(16) | lower-case `8-4-4-4-12` string |
| FLOAT16 | FIXED_LEN_BYTE_ARRAY(2) | float |
| INTERVAL | FIXED_LEN_BYTE_ARRAY(12) | `['months' => int, 'days' => int, 'milliseconds' => int]` |
| anything else (VARIANT, GEOMETRY, GEOGRAPHY, future annotations, UNKNOWN) | | the physical value |

A logical type wins over the converted type when both are present. An annotation that does not fit the physical type (a DATE on INT64, a UUID on INT32) is ignored: you get
the physical values. Years outside 0000-9999 are written with a `-` when negative and with all their digits otherwise (`-0001-12-31`, `10000-01-01`); proleptic Gregorian calendar.
A time outside one day is written as it is (`25:00:00.000`, with a `-` when negative). Decimals stored in more than 128 bytes throw `UnsupportedException`.

## Writing

<!-- example: writer -->
```php
use Charm\Parquet\{Type, TimeUnit, Writer};

$w = new Writer([
    'id'      => Type::int64()->required(),
    'name'    => Type::string(),
    'price'   => Type::decimal(18, 2),
    'seen'    => Type::timestamp(TimeUnit::Micros, utc: true),
    'tags'    => Type::list(Type::string()),
    'address' => Type::struct(['city' => Type::string(), 'zip' => Type::string()]),
]);
$out = fopen('out.parquet', 'wb');
foreach ($rows as $row) {
    fwrite($out, $w->encode($row));   // '' most of the time; the magic with the first row, a whole row group when one fills
}
fwrite($out, $w->finish());          // the last row group, the page indexes and the footer; the writer is closed
fclose($out);
```

The writer returns bytes; it opens no file and buffers one row group. `encode()` returns the bytes that became final, `finish()` closes the writer (every later call throws
`\Error`); a writer that got no row produces a valid file with no row group. `rowCount()` is the number of rows accepted so far.
`new Writer($reader->schema())` writes with the schema of a file you read (see below).

### An array of rows to a file in five lines

<!-- example: infer -->
```php
use Charm\Parquet\{Type, Writer};

$w = new Writer(Type::infer($rows));            // $rows: an array of [column => value] arrays
$bytes = '';
foreach ($rows as $row) {
    $bytes .= $w->encode($row);
}
file_put_contents('out.parquet', $bytes . $w->finish());
```

`Type::infer()` looks at the rows you give it (all of them, or a sample: a row that later contradicts the schema is a `\ValueError` from `encode()`) and returns the `['name' => Type]` array for the Writer.
Everything is nullable. PHP `bool` is boolean, `int` is int64, `float` is double, a `string` is a string when it is valid UTF-8 (a column with an invalid one becomes bytes),
`\DateTimeInterface` is timestamp(micros, UTC), a list is a list of the element type merged over all its elements and rows, and an array with string keys is a struct whose fields are merged over all rows
(a field missing from some rows is just null there). int and float merge to double; null merges with anything. A column that is only ever null becomes a nullable string, and one that is only ever an empty array a list of strings.
A conflict (int and string, a list and a struct, ...) is a `\ValueError` naming the column path and the row, an object that is not a `\DateTimeInterface` a `\TypeError`.
It never infers decimals (a float is a double, a numeric string a string), dates or times from strings, maps (an array with integer keys that is not a list is an error), narrower integers, uuids or json:
for those columns write the `Type` yourself and merge it into the array (`['id' => Type::int32()->required()] + Type::infer($rows)`).

### Types

`Type` is immutable; columns are optional (nullable) unless you call `->required()`.

| Constructor | Written as (physical, annotation) |
|---|---|
| `boolean()` | BOOLEAN |
| `int8()`, `int16()`, `int32()`, `int64()` | INT32 / INT32 / INT32 / INT64 with INT(8/16/32/64, signed) and the converted type |
| `uint8()`, `uint16()`, `uint32()`, `uint64()` | INT32 / INT32 / INT32 / INT64 with INT(w, unsigned) and the converted type |
| `float()`, `double()`, `float16()` | FLOAT, DOUBLE, FIXED_LEN_BYTE_ARRAY(2) with FLOAT16 |
| `string()`, `json()` | BYTE_ARRAY with STRING / JSON (UTF-8 is validated) |
| `bytes()`, `fixed($n)` | BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY(n) |
| `decimal($precision, $scale = 0)` | INT32 up to precision 9, INT64 up to 18, otherwise FIXED_LEN_BYTE_ARRAY of the smallest length (up to precision 38) with DECIMAL |
| `date()` | INT32 with DATE |
| `time($unit, utc: false)`, `timestamp($unit, utc: false)` | INT32 (TIME in milliseconds) or INT64, with TIME / TIMESTAMP and the unit (`TimeUnit::Millis`, `Micros`, `Nanos`), `isAdjustedToUTC` = `$utc`; timestamps in ms and us also get the converted type |
| `uuid()` | FIXED_LEN_BYTE_ARRAY(16) with UUID |
| `list($element)` | the standard 3-level LIST (`list` / `element`) |
| `struct(['name' => Type, ...])` | a group |
| `map($key, $value)` | the standard MAP (`key_value` / `key` / `value`); the key is always required |

Every annotation is written as the logical type and, where one exists, the legacy converted type next to it. A row is `['column' => value]`; a missing key is `null`, an unknown key is an error.
`new Writer($reader->schema())` accepts the schema of a file you read; legacy list and map forms are written back as the standard forms, so the rows are the same but the schema bytes are not.
What the writer cannot express throws `UnsupportedException` naming the column: INT96, ENUM, BSON, INTERVAL, a MAP without a value column, a group without fields.

### Accepted values

The values `Reader::rows()` returns in logical mode are accepted as they are; some more are accepted for convenience. The first column is the rule, the last what is refused.

| Type | Accepted | Refused with |
|---|---|---|
| boolean | `bool` | `TypeError` for anything else |
| intN, uintN | `int` within the range; `uint64` also a decimal digit string up to 2^64-1 | `TypeError` for a float, a numeric string or a bool; `ValueError` out of range |
| float, double | `float` or `int` (a float is rounded to the nearest float32 for `float()`) | `TypeError` |
| float16 | `float` or `int` (rounded to nearest even) | `TypeError`; `ValueError` beyond 65504 |
| string, json | `string`, valid UTF-8 (the Reader returns the bytes of a string column unvalidated, so a file written by another program may hold strings the Writer refuses; use `Type::bytes()` to write arbitrary bytes) | `TypeError`; `ValueError` for invalid UTF-8 |
| bytes | `string` | `TypeError` |
| fixed(n) | `string` of exactly n bytes | `ValueError` for another length |
| decimal | `string` (`"-123.45"`, `"7"`, `"+0.5"`) or `int`, with at most `scale` fractional digits and at most `precision` digits; nothing is rounded | `TypeError` for a float; `ValueError` for text that is no number, extra digits, too many digits |
| date | `"YYYY-MM-DD"`, a `\DateTimeInterface` (its calendar date as the object has it), or an `int` of days since 1970-01-01 | `TypeError`; `ValueError` for a day that does not exist or beyond 32 bits |
| time | `"HH:MM:SS[.fraction]"` (more fractional digits than the unit holds is refused, hours above 23 and a leading `-` are accepted because the Reader returns them), or an `int` in the unit | `TypeError`; `ValueError` |
| timestamp | `"YYYY-MM-DDTHH:MM:SS[.fraction][Z]"` (`Z` only when `utc: true`), a `\DateTimeInterface` (its instant, truncated to the unit), or an `int` in the unit | `TypeError`; `ValueError` for a bad date, a `Z` on a local type, an instant beyond 64 bits |
| uuid | `string` in the 8-4-4-4-12 form, any case | `TypeError`; `ValueError` |
| list | a PHP list | `TypeError` for a non-array; elements are checked as their type |
| struct | an array keyed by field name; a missing field is `null` | `TypeError` for a non-array; `ValueError` for an unknown field or null in a required field |
| map | an associative array (`['a' => 1]`; an int key of a string-keyed map is turned into a string) or a list of `[key, value]` pairs (needed for non-string keys, or for a key order the PHP array cannot keep) | `TypeError`; `ValueError` for a null key |

`null` is accepted where the type is optional. A refused row throws `\TypeError` (wrong PHP type) or `\ValueError` (right type, value does not fit); the message names the column path and the row
(`column 'address.zip', row 41: expected string, got int`). The row is converted completely before any of it is buffered, so the writer is unchanged by a refused row and you can go on.

### Options

`new Writer($schema, codec: ..., ...)` (all named, all optional; **pass them by name**: the order of the parameters after the first is not part of the contract, and the same goes for the `Reader` constructor and its factories):

| Option | Default | Meaning |
|---|---|---|
| `codec` | `Codec::Snappy` | `Uncompressed`, `Snappy` (pure PHP, a greedy hash matcher), `Gzip` (ext-zlib, level 6), `Zstd` (charm/zstd's compressor: Huffman-coded literals and FSE-coded sequence tables, a fast single-pass match finder; see below); any other codec is a `\ValueError`, `Gzip` without ext-zlib too |
| `dataPageSize` | 1 MiB | target size of a data page in bytes of PLAIN-encoded values; a page always holds whole rows |
| `rowGroupRows` | 1,048,576 | a row group is written when it has this many rows ... |
| `rowGroupBytes` | 32 MiB | ... or when the buffered values are estimated at this many bytes of PHP memory (16 bytes per value or level, 48 + length per string) |
| `dictionary` | true | RLE_DICTIONARY with a PLAIN dictionary page where the type allows (not for booleans, floats and doubles); a column falls back to PLAIN from the page where its dictionary would exceed `dictionaryPageSize` |
| `dictionaryPageSize` | 1 MiB | |
| `pageVersion` | 1 | 1 or 2 |
| `statistics` | true | min, max (flagged exact), null count per chunk; the column index per page |
| `pageIndex` | true | the offset index (and, with `statistics`, the column index), which `Reader::candidateRanges()` and row ranges use |
| `keyValueMetadata` | `[]` | file-level key/value pairs |

What it writes: data pages with PLAIN or RLE_DICTIONARY values and RLE levels, `created_by` = `charm/parquet version x.y.z`, column order TYPE_DEFINED_ORDER. Statistics follow the ordering of each type: unsigned
integers and byte strings compare as unsigned bytes, decimals in fixed bytes as signed big-endian, floats leave NaN out (a column of only NaN has no min and max) and a zero minimum is written as -0.0, a zero maximum as +0.0.
Byte-string statistics are not truncated; a value longer than 4096 bytes leaves min and max out for that column. The page index lists the data pages; a page of a repeated column always starts a row.

Not written: encryption, bloom filters, DELTA_* and BYTE_STREAM_SPLIT encodings, ALP, LZ4, Brotli and LZO, INT96, legacy list/map forms, sorting columns, size statistics, geospatial statistics.

### Row groups, memory and file size

The writer keeps one row group in PHP arrays: about 230 bytes of memory per buffered row of a 5-column schema (int64, string, double, timestamp, boolean; the estimate the limit uses is 185) and 1.5 to 2 times that while a
group is encoded. Peak memory with the default options is therefore about twice `rowGroupBytes`: measured over the start 70 MB for the 5-column schema (a group of 181,000 rows) and 42 MB for a 50-column one (13,000 rows), under PHP's default
`memory_limit` of 128M. 32 MiB is the largest limit for which that holds with a margin; raise `rowGroupBytes` for larger row groups (a better ratio and fewer groups to read) if the memory is there. 2 million rows of the 5-column schema in groups of
50,000 rows peak at 21 MB (`WriterMemoryTest`).

Compared with pyarrow 26 on the datasets of `bench/` (`php bench/run.php --write`, `bench/pyarrow-write.py`; one million rows unless noted; pyarrow's row groups hold up to 1M rows and its page index is off by default):

Output size in kilobytes, ours / pyarrow's (defaults of both, the page index only in ours):

| dataset | uncompressed (ours / pyarrow, KB) | snappy (ours / pyarrow, KB) | gzip (ours / pyarrow, KB) | zstd (ours / pyarrow, KB) |
|---|---|---|---|---|
| int64-plain | 8248 / 8273 | 8228 / 8003 | 6442 / 6439 | 6276 / 6599 |
| int64-dict | 1258 / 1264 | 1255 / 1260 | 1253 / 1259 | 1252 / 1258 |
| double | 8000 / 8273 | 8000 / 8273 | 7543 / 7802 | 7522 / 7782 |
| str-lowcard | 1011 / 1008 | 1004 / 1006 | 967 / 971 | 967 / 1006 |
| str-highcard | 23267 / 23096 | 14730 / 12420 | 8200 / 7822 | 8789 / 8295 |
| opt-10pct-null | 7595 / 7625 | 6515 / 5831 | 4652 / 4656 | 4492 / 4167 |
| decimal | 8248 / 6373 | 8011 / 6374 | 6011 / 5895 | 5794 / 5895 |
| timestamp | 8248 / 8273 | 8248 / 8273 | 7173 / 7187 | 7306 / 7322 |
| list-int32-tokens | 2565 / 2367 | 2508 / 2283 | 2309 / 2181 | 2312 / 2195 |
| struct-3 | 3478 / 3742 | 3472 / 3739 | 3311 / 3563 | 3302 / 3560 |
| wide-50 | 4170 / 4491 | 3642 / 3874 | 3097 / 3418 | 3090 / 3464 |

The sizes are close where the data decides them (dictionary-friendly or incompressible columns), and smaller than pyarrow's for double (pyarrow tries a dictionary first and falls back), struct-3 and wide-50. Snappy is 2 to 19 percent larger than pyarrow's on text, nulls and integers with structure (the greedy matcher finds fewer matches than the reference). Zstd, with charm/zstd's Huffman and FSE coding, is
within -5 and +8 percent of pyarrow's Zstd everywhere (smaller on int64-plain, decimal, struct-3, wide-50; 8 percent larger on opt-10pct-null, 6 on str-highcard) and 4 to 40 percent smaller than our Snappy where there is anything to compress: str-highcard 14.7 to 8.8 MB, opt-10pct-null 6.5 to 4.5, decimal 8.0 to 5.8, int64-plain 8.2 to 6.3, timestamp 8.2 to 7.3. (Before charm/zstd got entropy coding, Zstd here was up to 50 percent larger than pyarrow's and saved almost nothing over Snappy.)
`list-int32-tokens` is 8 percent larger than pyarrow's because the memory limit cuts it into 2 row groups of 70,000 rows (pyarrow: one of 100,000) and each group has its own dictionary. Decimal columns are rewritten with the physical type their precision picks (INT64 for decimal(15, 2) where pyarrow's file used 7 bytes), hence the larger uncompressed file.
The cost is time (the Reader has the same shape: it is bound by pure-PHP decoders):

Milliseconds to encode and write the whole dataset (best of 3, after the speed pass below, after a warm-up; this machine, PHP 8.5 with the tracing JIT / without), rows per second with the JIT, MB/s of output for Snappy, and pyarrow's milliseconds for the same write from an Arrow table:

| dataset | rows | uncompressed: ms JIT / no JIT (rows/s JIT) | snappy: ms JIT / no JIT (rows/s JIT, MB/s out) | zstd: ms JIT / no JIT | gzip: ms JIT | pyarrow ms (uncompressed / snappy) |
|---|---|---|---|---|---|---|
| int64-plain | 1000000 | 170 / 276 (5,867,780) | 435 / 721 (2,296,086, 18.0) | 555 / 1059 | 643 | 7 / 13 |
| int64-dict | 1000000 | 173 / 304 (5,773,543) | 175 / 305 (5,701,902, 6.8) | 182 / 311 | 189 | 7 / 7 |
| double | 1000000 | 170 / 265 (5,864,523) | 173 / 267 (5,767,024, 44.0) | 338 / 553 | 312 | 7 / 7 |
| str-lowcard | 1000000 | 361 / 503 (2,764,194) | 363 / 505 (2,748,909, 2.6) | 368 / 521 | 347 | 12 / 13 |
| str-highcard | 1000000 | 391 / 521 (2,555,548) | 941 / 1430 (1,061,856, 14.9) | 1270 / 2199 | 1033 | 11 / 37 |
| opt-10pct-null | 1000000 | 165 / 270 (6,049,466) | 400 / 658 (2,493,963, 15.5) | 493 / 902 | 490 | 10 / 17 |
| decimal | 1000000 | 336 / 498 (2,969,931) | 630 / 983 (1,585,298, 12.1) | 710 / 1288 | 737 | 16 / 16 |
| timestamp | 1000000 | 525 / 705 (1,901,402) | 532 / 713 (1,877,860, 14.8) | 693 / 1013 | 859 | 7 / 7 |
| list-int32-tokens | 100000 | 163 / 288 (610,425) | 166 / 295 (601,190, 14.4) | 175 / 323 | 221 | 11 / 11 |
| struct-3 | 333333 | 371 / 494 (897,238) | 374 / 495 (890,333, 8.8) | 433 / 594 | 425 | 13 / 13 |
| wide-50 | 20000 | 156 / 213 (127,994) | 199 / 280 (100,290, 17.4) | 271 / 401 | 256 | 15 / 17 |


pyarrow needs 7 to 37 ms for these, so the pure-PHP writer is 20 to 60 times slower for uncompressed and Snappy output; the real cost is building the PHP rows and converting each value (the dictionary datasets take 170 ms for a million rows however they are compressed). GZIP is the one codec where this writer wins: pyarrow's default level makes it take seconds (3 s for int64-plain) where zlib level 6 takes 0.7 s. Snappy costs about 250 ms per 8 MB of int64 data (30 MB/s). Zstd costs 1.0 to 2.0 times the Snappy time (1.3 times on int64-plain and timestamp, 1.35 on str-highcard, 1.9 on double, whose bytes it cannot shrink: the histogram test and the match finder run for nothing), so on these datasets Zstd is the better choice when size matters and Snappy when the writing time does.

The speed pass (rule: keep a change only if it is at least 10 percent faster on a workload that matters and nothing is more than 5 percent slower; best of 5 to 7, JIT on, `bench/write.php`):

* **Kept: a flat-schema path** in `Writer::encode()` for schemas of top-level scalar columns: no staging arrays, the per-column facts in one table, the unknown-column check folded into the conversion loop, the page-size estimate of fixed-width columns computed once per chunk instead of per value. Uncompressed, ms before to after: wide-50 165 to 122, double 211 to 156, opt-10pct-null 200 to 177, int64-dict 203 to 172, int64-plain 206 to 170, decimal 390 to 336, struct-3 and str-lowcard 3 percent (nested and string-heavy paths were not changed); nothing slower (without the JIT 2 to 14 percent faster).
* **Kept: a day-count cache and a direct fraction parse in the timestamp string conversion**: timestamp 622 to 555 ms (11 percent).
* **Rejected: a faster Snappy inner loop** (a rolling 4-byte word, byte compares by `ord()`, 8-byte match extension, the hash table seeded two bytes back): int64-rising 27.7 to 25.5 ms per MB (8 percent), PLAIN strings 21.2 to 24.3 (14 percent slower), dictionary indices 30.7 to 32.0 (4 percent slower, 2 percent smaller output). Not kept: nothing reached 10 percent and one workload got slower by more than 5.
* **Not tried further:** a dictionary encoder that maps values to ids in C (`array_flip` / `array_intersect_key` do not preserve what is needed), and `ctype_print()` as a shortcut for the UTF-8 check (45 ns of a 250 ns string conversion; `ext-ctype` is not required by the package).

### How the output was checked

* `WriterTest`: 40 seeded random schemas (all types, nesting to depth 4, required and optional at every level, edge values: the integer limits, NaN, -0.0, empty strings and lists, decimals at the precision limit, dates and timestamps at the limits of 64 bits) x 12 configurations (every codec, both page versions, dictionary on and off, 1-row pages and groups, tiny dictionary pages, the defaults); the
  rows must come back from this package's Reader identical in type and value.
* `php tests/tools/roundtrip.php --seeds=40` writes the same 280 files and has pyarrow and DuckDB read them: both must give the written values; the footer statistics must equal the true min, max and null count (pyarrow computes them) and the statistics pyarrow writes for the same data; the offset index must be present;
  `read_row_group` must equal slices of the whole read; and range filters (`pyarrow.parquet.read_table(filters=)` and DuckDB `WHERE x BETWEEN`) must return the rows a brute-force count says. Result with 60 seeds, 420 files: own Reader 0 mismatches; pyarrow 420 OK; DuckDB 329 OK, 91 KNOWN (below), 0 FAIL; statistics, indexes and filters 420 OK
* `php tests/tools/rewrite.php` rewrites every `data/` file of Apache's parquet-testing that the Reader can read with `new Writer($reader->schema())`, compares the rows read back with the original ones and has pyarrow and DuckDB read the result. Of the 83 `.parquet` files, 72 are rewritten and read back identically by this Reader, pyarrow and DuckDB (statistics, page index, row group reads and filters included); 9 throw `UnsupportedException` (7 INT96 timestamp columns: `alltypes_*`, `int96_*`; `bson.parquet`; `map_no_value.parquet`); 2 are not readable by the Reader to begin with (ALP; the 2 GB Brotli file, whose page is above the page size limit); 0 mismatches. The 14 encrypted files of `data/` are not part of the 83.
* `php tests/tools/snappy.php`: 319 inputs (the unit-test inputs and 300 random ones, 29.6 MB, compressed to 5.9 MB) decoded by the reference python `snappy` package and by pyarrow's Snappy codec: 0 mismatches each.
* `InferTest` (inference of every kind, then write and read back; every conflict message) and `ReadmeExamplesTest` (the README's PHP examples are run in a temporary directory, the `Reader` signature listing is compared with the class by reflection: an example that shows a method that does not exist fails the suite).
* `WriterErrorTest` (every refusal, the message, that the writer is unchanged and byte-identical to a clean run afterwards), `WriterFormatTest` (RLE, Snappy, footer, statistics, page index and encodings), `WriterMemoryTest` (2 million rows).
* 20 faults injected into the writer one at a time (a definition level off by one, the dictionary fallback keeping the ids of dropped entries, min and max swapped, an unsigned maximum compared as signed, a wrong `first_row_index`, a wrong null count, an RLE run off at an 8-value boundary, footer offsets off by the magic length,
  a Snappy copy of 65 bytes, a lost -0.0, an unsigned decimal order, a wrong row number in a message, a v2 `num_rows` of entries instead of rows, swapped boundary order, a map entry's repetition level, no flush on the memory estimate, a refused row that leaves a value behind, a null count missing for all-null pages, a page that starts inside a record, NaN in the statistics): all 20 are caught; 5 of them were not caught by the first version of the tests and led to new ones.
* Disagreements with the other readers, all of them DuckDB's, none changes the file: (1) a TIMESTAMP(MILLIS) far from the epoch makes DuckDB raise `Could not convert Timestamp(MS) to Timestamp(US)` (28 of the 420 files; it does the same for a file pyarrow wrote with `2**62` milliseconds); (2) DuckDB has microsecond TIME and drops the sub-microsecond digits of TIME(NANOS) and fraction digits of TIMESTAMP in some units (63 files); the roundtrip tool
  counts both as KNOWN, comparing the rest of the value. pyarrow read every file the writer produced. The checks do not compare pyarrow's Python objects for datetimes outside Python's range or for float16: those columns are compared through their storage by `oracle.py` instead.

## What is supported, and what throws `UnsupportedException`

* **Format:** the complete footer as value objects; data pages v1 and v2; dictionary pages; page indexes (offset index and column index); nested schemas of any depth.
* **Encodings:** PLAIN, PLAIN_DICTIONARY, RLE_DICTIONARY, RLE (booleans, levels), BIT_PACKED (levels), DELTA_BINARY_PACKED, DELTA_LENGTH_BYTE_ARRAY, DELTA_BYTE_ARRAY, BYTE_STREAM_SPLIT.
* **Codecs:** UNCOMPRESSED, SNAPPY (pure PHP), GZIP (ext-zlib), ZSTD (charm/zstd), LZ4_RAW and LZ4 (pure PHP; for LZ4 Hadoop framing, then a plain block, then an LZ4 frame), BROTLI (ext-brotli when it is loaded, else charm/brotli in pure PHP).
  Every page must decompress to exactly the size its header states.
* **`UnsupportedException`:** the encoding ALP; the codec LZO; encrypted files and encrypted columns; columns stored in another file;
  a chunk or page above the limits below; decimals above 128 bytes. Ids this version does not know (a codec, an encoding, a converted type) only make the column that needs them unreadable.

## Errors and limits

Every failure caused by the file is a `ParquetException` (corrupt, truncated, violates the format) or its subclass `UnsupportedException` (valid, not implemented; the message names it).
`ParquetException` extends `\UnexpectedValueException`, the class every charm package uses for bad input data, so one `catch (\UnexpectedValueException)` covers this package and charm/zstd and charm/brotli.
The Reader throws nothing else for any input bytes, and raises no PHP warnings (the zlib functions are called with their warnings swallowed locally). A codec's own failure (an `\UnexpectedValueException`
from charm/zstd or charm/brotli) becomes a `ParquetException` with the original as its previous exception.

The rest follows the rule PHP's own extensions use: a wrong call throws a built-in error, bad data a `\UnexpectedValueException`, and I/O a `\RuntimeException`.

| What went wrong | Thrown |
|---|---|
| The file is corrupt, truncated or violates the format | `ParquetException` |
| The file is valid but uses something this version does not implement | `UnsupportedException` (a `ParquetException`) |
| Misuse: an unknown column, a row group out of range, a negative offset or limit, a malformed row range, an invalid `Writer` option (including a codec it cannot write) | `\ValueError` |
| Misuse: a row the Writer refuses (see "Accepted values") | `\TypeError` or `\ValueError` |
| Misuse: a call in the wrong state (any call to a finished `Writer`) | `\Error` |
| The environment: 32-bit PHP (the `Reader` and `Writer` constructors refuse to start); `Codec::Gzip` for the `Writer` without ext-zlib | `\Error`; `\ValueError` |
| `Reader::fromFile()` on a file that is missing or cannot be read | `\RuntimeException` |
| The range function returns something other than exactly `$length` bytes | `\UnexpectedValueException` |
| The range function itself throws | that exception, unchanged: the Reader neither catches nor wraps it |

Sizes and counts that come from the file are checked before anything is allocated: page sizes against the column chunk, value counts against the row group, a page's decompressed size against
its header, level values against the schema maxima, offset index entries against the chunk and file, and the lengths of index blobs against the file size.

* `maxPageSize` (default 1 GiB): the largest uncompressed page accepted. It stops a page header that claims gigabytes (and, for zstd and Brotli, is passed as the decompression bound, so a decompression bomb is cut off after one block, not after it has been inflated).
* `maxChunkValues` (default 2^25 = 33.5 million): the most level entries in one column chunk (rows, for a flat column). A column chunk that claims more is refused with `UnsupportedException` before any array is allocated; it is the memory knob (a PHP array of that many
  values needs about 0.5 GB), and PHP's `memory_limit` is the real backstop for legitimate large chunks.

## Performance

Measured with `bench/bench.php` (through `bench/run.php`) on PHP 8.3.36 (opcache on), Linux 6.17, AMD Ryzen 9 9950X3D (32 threads). **The machine was not idle** (another job was running, load average 2 to 10 during the measurements), so treat differences below 5 to 10% as noise.
Files are written by pyarrow 26 (`bench/make-data.py`, about 1 million values each, 500,000-row row groups, snappy and zstd). Each workload reads every column of every row group once (`readColumn`, or `rows()` for the wide table) after a warm-up read; the time is the best of 7.
"MB/s" is megabytes (2^20) of **file** per second, "Mval/s" million values (level entries, so a list of 10 ints counts 10) per second. Values are logical unless the workload says physical. The "wide-50, 3 columns" row reads 3 of 50 columns (60,000 values) so its MB/s relates to the whole file.

| workload | codec | file MB | JIT ms | MB/s | Mval/s | no JIT ms | MB/s | Mval/s |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| int64-plain | snappy | 7.4 | 50 | 149 | 20.2 | 77 | 95 | 12.9 |
| int64-plain | zstd | 6.0 | 89 | 68 | 11.2 | 216 | 28 | 4.6 |
| int64-dict | snappy | 1.2 | 16 | 75 | 62.6 | 32 | 38 | 31.3 |
| int64-dict | zstd | 1.2 | 16 | 74 | 61.8 | 35 | 35 | 28.9 |
| int64-delta | snappy | 0.5 | 13 | 39 | 78.7 | 22 | 22 | 44.6 |
| int64-delta | zstd | 0.4 | 19 | 20 | 52.4 | 40 | 10 | 24.8 |
| double | snappy | 7.6 | 21 | 357 | 46.7 | 21 | 364 | 47.6 |
| double | zstd | 7.2 | 99 | 73 | 10.1 | 230 | 31 | 4.3 |
| str-lowcard | snappy | 1.0 | 16 | 59 | 60.9 | 27 | 35 | 36.9 |
| str-lowcard | zstd | 1.0 | 17 | 58 | 60.5 | 30 | 32 | 33.6 |
| str-highcard | snappy | 11.8 | 143 | 82 | 7.0 | 252 | 47 | 4.0 |
| str-highcard | zstd | 7.8 | 199 | 39 | 5.0 | 542 | 14 | 1.8 |
| str-delta | snappy | 2.8 | 70 | 40 | 14.2 | 136 | 21 | 7.4 |
| str-delta | zstd | 2.0 | 84 | 24 | 11.9 | 191 | 11 | 5.2 |
| opt-10pct-null | snappy | 5.3 | 69 | 77 | 14.6 | 117 | 45 | 8.5 |
| opt-10pct-null | zstd | 3.7 | 88 | 42 | 11.4 | 245 | 15 | 4.1 |
| opt-90pct-null | snappy | 0.7 | 21 | 33 | 46.8 | 38 | 19 | 26.5 |
| opt-90pct-null | zstd | 0.5 | 27 | 20 | 36.6 | 62 | 9 | 16.3 |
| decimal | snappy | 5.7 | 89 | 65 | 11.3 | 138 | 41 | 7.2 |
| decimal | zstd | 5.3 | 147 | 36 | 6.8 | 296 | 18 | 3.4 |
| timestamp | snappy | 7.6 | 138 | 55 | 7.2 | 169 | 45 | 5.9 |
| timestamp | zstd | 6.7 | 213 | 32 | 4.7 | 372 | 18 | 2.7 |
| list-int32-tokens | snappy | 2.4 | 65 | 36 | 15.4 | 103 | 23 | 9.7 |
| list-int32-tokens | zstd | 2.2 | 70 | 31 | 14.2 | 121 | 18 | 8.3 |
| list-float-128 | snappy | 3.9 | 52 | 76 | 19.9 | 65 | 61 | 15.9 |
| list-float-128 | zstd | 3.5 | 89 | 39 | 11.4 | 170 | 21 | 6.0 |
| struct-3 | snappy | 3.8 | 49 | 78 | 20.4 | 76 | 50 | 13.1 |
| struct-3 | zstd | 3.6 | 79 | 46 | 12.7 | 161 | 23 | 6.2 |
| decimal (physical) | snappy | 5.7 | 15 | 388 | 67.8 | 15 | 373 | 65.1 |
| decimal (physical) | zstd | 5.3 | 73 | 73 | 13.7 | 167 | 32 | 6.0 |
| timestamp (physical) | snappy | 7.6 | 21 | 360 | 47.1 | 21 | 365 | 47.8 |
| timestamp (physical) | zstd | 6.7 | 95 | 71 | 10.5 | 226 | 30 | 4.4 |
| wide-50, rows() all columns | snappy | 3.7 | 40 | 92 | 24.9 | 66 | 56 | 15.2 |
| wide-50, rows() all columns | zstd | 3.3 | 66 | 49 | 15.0 | 156 | 21 | 6.4 |
| wide-50, rows() 3 columns | snappy | 3.7 | 3 | 1140 | 18.4 | 6 | 670 | 10.8 |
| wide-50, rows() 3 columns | zstd | 3.3 | 4 | 812 | 14.8 | 9 | 370 | 6.8 |

The JIT is the tracing JIT (`opcache.jit=1255`, 128 MB buffer); "no JIT" is opcache with `opcache.jit_buffer_size=0`. Reproduce with:

```
research/venv/bin/python bench/make-data.py       # once; needs pyarrow; writes about 120 MB into bench/data/ (git-ignored)
php bench/run.php                                  # --runs=7, --filter=text, --codec=snappy|zstd, --out=prefix (then bench/compare.php a.json b.json)
```

The zstd rows above were measured again after charm/zstd's compressor changed (3 runs): they read files pyarrow wrote, so only the decoder counts, and nothing moved (int64-plain 88 ms against 89, str-highcard 200 against 199, timestamp 209 against 213).

Most of the time of a snappy file is the pure-PHP Snappy decoder, and of a zstd file charm/zstd's decoder: the same random 7.4 MB int64 column reads in 18 ms uncompressed, 48 ms snappy and 93 ms zstd (JIT, before the tuning below).
For context only, pyarrow 26 on the same files (`bench/pyarrow-times.py`; a different runtime doing different work: native decoding into Arrow arrays, then a conversion to Python objects): `pq.read_table()` alone takes 10 to 127 ms
(for example int64-plain 74, str-highcard 127, list-int32 29), and `read_table().column('v').to_pylist()` takes 18 to 1649 ms (int64-plain 92, str-highcard 161, decimal 416, timestamp 1649, list-int32 54);
`read_table().to_pylist()` of the wide table takes 93 ms with all columns and 8 ms with three.

## Limitations

* The writer does not write encryption, bloom filters, delta encodings, ALP, BYTE_STREAM_SPLIT, LZ4/Brotli/LZO, INT96, ENUM, BSON, set-like maps (a MAP without values) or the legacy list forms (see Writing).
* No encryption (encrypted footers and columns throw `UnsupportedException`), no ALP encoding, no LZO; BROTLI pages are read with `ext-brotli` when it is loaded (that path is not tested here: the extension is not installed on the development machine) and with charm/brotli otherwise (tested, against pyarrow and DuckDB files).
* No bloom filters, and no filtering beyond `candidateRanges()` (one column, one interval, plus null); the caller filters the rows it gets.
* One row group in memory at a time in `rows()`, and a column chunk is decoded whole (or, with an offset index and a row range, the pages that hold the range); there is no streaming within a page.
* A row range over a column chunk without an offset index decodes the whole chunk and slices it.
* Pure PHP: it is much slower than a native reader, and with the pure-PHP Snappy, charm/zstd and charm/brotli it is bound by those decoders (see Performance). `ext-snappy` and `ext-zstd` are not used; `ext-brotli` is used when loaded.
* PHP integers: 64-bit PHP is required (32-bit PHP is not supported). UINT_64 values above `PHP_INT_MAX` come back as decimal strings. FLOAT values are doubles (exact); NaN payloads are not preserved.
* Dates and timestamps are strings, not objects, and the calendar is proleptic Gregorian written by integer arithmetic; a year outside 0000-9999 has a sign or more digits.
* An annotation that does not fit its physical type is ignored; a decimal wider than 128 bytes is refused; a MAP key that is not an int or a string gives pairs (or use `MapMode::Pairs`).
* Page index pruning is conservative on purpose (see `candidateRanges`): a statistic the footer flags inexact, the deprecated min/max of strings and unsigned integers, NaN, and INT96 and INTERVAL columns are never used to rule rows out.
* The library does not check page CRCs, bloom filters, or the Parquet column order declarations beyond what the statistics fields say.

## Tests

```
composer install
vendor/bin/phpunit            # no pyarrow and no corpus needed
```

(PHPUnit 11 needs `ext-mbstring`; the 8.3 on the development machine had none, so the suite was run with PHP 8.5 and, through a stub runner, with 8.3.)
The suite has 825 tests (about 282,000 assertions) and takes about 26 seconds. CI runs PHP 8.2 to 8.5; 8.2 was not run on the development machine.

* **Fixtures** in `tests/fixtures/` (3.3 MB), written by pyarrow with their expected values (JSON, read back by pyarrow; regenerate with `research/venv/bin/python tests/fixtures/generate.py`): codecs x page versions x dictionary on/off over
  all physical types, tiny pages, dictionary fallback, multiple row groups, null patterns, empty files, every logical type, every nested shape (lists, structs, maps, depth six, a 100,000-element row), DELTA and BYTE_STREAM_SPLIT encodings, LZ4_RAW,
  and page-indexed files for the row range and candidate range tests.
* **Hand-built files** (`FileBuilder`, `NestedFileBuilder`): hostile sizes, counts, offsets and level streams; legacy list and map forms; rows spanning data pages; hostile offset and column indexes.
* **Independent expectations:** logical values are checked against pyarrow's integer storage formatted by separate Python code (and asserted against Arrow's own string casts when the fixtures are generated); decimals of 0 to 32 bytes, dates, timestamps and LZ4 streams against vectors made by
  Python's `int.from_bytes`, `datetime` and the `lz4` package; candidate ranges against a brute-force scan; statistics against the true min and max pyarrow computes from the data.
* **Corpus conformance:** `php tests/tools/conformance.php --mode=physical|logical` reads every file of Apache's `parquet-testing` (`data/` and `bad_data/`) with the library and with pyarrow and compares every value (flat columns one by one; nested top-level fields as assembled trees).
  Result with pyarrow 26.0.1: of the 97 files of `data/`, 78 PASS, 16 are `UnsupportedException` (11 encrypted footers, 3 encrypted columns, 1 ALP, 1 Brotli file, `large_string_map.brotli.parquet`, whose page of 1 GiB + 4 bytes is above the page size limit) and 3 cannot be read by pyarrow itself; of the 23 files of `bad_data/`, 8 are rejected with a `ParquetException` and 15 are read with values identical to pyarrow's
  (pyarrow accepts them too); 0 differences and 0 errors in either mode. Each mode compared 503 flat columns (323,875 values) and 61 nested top-level fields. Where pyarrow and this library differ by convention (`map_no_value`, a key-only map) or pyarrow is wrong (`empty_data_pages`), the oracle script says so.
* **Fuzzing and mutation checks:** seeded truncations and bit flips (`CorruptionTest`, and outside PHPUnit over every corpus and fixture file; the final run was 288,000 mutated files, with row ranges and candidate ranges included) must give a `ParquetException` or a successful read, never a warning or another Throwable. Faults were injected into the code one at a time and the suite run: 41 faults over the sprints
  (decoders, level meaning, assembly, page selection, statistics pruning, bounds checks); each is caught by a named test, and 8 of them were not caught at first and led to new tests.

## License

MIT
