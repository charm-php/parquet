<?php

declare(strict_types=1);

namespace Charm\Parquet;

use Charm\Parquet\Internal\Write\{ChunkBuffer, ChunkEncoder, Compile, SchemaToType, Stats, WNode, WriteOptions};
use Charm\Parquet\Thrift\CompactWriter as T;

/**
 * Writes a Parquet file from rows. Bytes out, no I/O: encode() returns the bytes that became final with the row (usually '', a whole
 * row group when one fills; the file magic comes with the first), finish() returns the last row group and the footer.
 *
 *     $w = new Writer(['id' => Type::int64()->required(), 'name' => Type::string()]);
 *     $out = fopen('out.parquet', 'wb');
 *     foreach ($rows as $row) {
 *         fwrite($out, $w->encode($row));
 *     }
 *     fwrite($out, $w->finish());
 *
 * A row is [column name => value] with the values {@see Reader::rows()} returns in logical mode for the type; see the README for what
 * else is accepted. A value of the wrong PHP type is a \TypeError, a value that does not fit (out of range, null in a required field, an
 * unknown key, a bad date) a \ValueError; both name the column and the row (counted from 0), and the writer stays usable.
 */
final class Writer
{
    public const VERSION = '0.1.0';

    private const MAGIC = 'PAR1';

    /** @var list<WNode> */
    private array $top;
    /** @var list<WNode> */
    private array $leaves;
    /** @var list<ChunkBuffer> */
    private array $buffers = [];
    private WriteOptions $options;
    private int $rowsInGroup = 0;
    private int $totalRows = 0;
    private int $memory = 0;
    private int $pos = 0;
    private bool $started = false;
    private bool $closed = false;
    private bool $flat = false;
    /** @var list<array{string, bool, \Closure, string, bool, bool, bool, ChunkBuffer}> flat schemas: name, nullable, converter, path, has definition levels, string-valued, variable width, buffer */
    private array $fCols = [];
    /** @var array<string, WNode> */
    private array $byName = [];
    /** @var list<array{rows: int, bytes: int, start: int, chunks: list<array<string, mixed>>}> */
    private array $groups = [];

    /**
     * @param Schema|array<string, Type> $schema column names and types, or the schema of a file read with Reader
     * @param Codec $codec UNCOMPRESSED, SNAPPY (default), GZIP or ZSTD (charm/zstd's compressor: valid output, weak ratio)
     * @param int $dataPageSize target size of a data page in bytes of PLAIN-encoded values (pages always hold whole rows)
     * @param int $rowGroupRows a row group is written when it holds this many rows ...
     * @param int $rowGroupBytes ... or this many bytes of PHP memory for the buffered values (an estimate), whichever comes first
     * @param bool $dictionary use RLE_DICTIONARY where the type allows, falling back to PLAIN when the dictionary page would exceed $dictionaryPageSize
     * @param int $pageVersion 1 or 2
     * @param bool $statistics write min/max/null count per column chunk and the column index
     * @param bool $pageIndex write the offset index (and, with statistics, the column index)
     * @param array<string, string> $keyValueMetadata file key-value metadata
     */
    public function __construct(
        Schema|array $schema,
        Codec $codec = Codec::Snappy,
        int $dataPageSize = 1 << 20,
        private readonly int $rowGroupRows = 1 << 20,
        private readonly int $rowGroupBytes = 32 << 20,
        bool $dictionary = true,
        int $dictionaryPageSize = 1 << 20,
        int $pageVersion = 1,
        bool $statistics = true,
        bool $pageIndex = true,
        private readonly array $keyValueMetadata = [],
    ) {
        if (\PHP_INT_SIZE < 8) {
            throw new \Error('64-bit PHP (PHP_INT_SIZE 8) is required');
        }
        if (!\in_array($codec, [Codec::Uncompressed, Codec::Snappy, Codec::Gzip, Codec::Zstd], true)) {
            throw new \ValueError('cannot write with codec ' . $codec->name . ' (UNCOMPRESSED, SNAPPY, GZIP and ZSTD are writable)');
        }
        if ($codec === Codec::Gzip && !\function_exists('gzencode')) {
            throw new \ValueError('cannot write with codec GZIP: ext-zlib is not loaded');
        }
        if ($pageVersion !== 1 && $pageVersion !== 2) {
            throw new \ValueError('pageVersion must be 1 or 2');
        }
        if ($dataPageSize < 1 || $rowGroupRows < 1 || $rowGroupBytes < 1 || $dictionaryPageSize < 1) {
            throw new \ValueError('sizes and row limits must be positive');
        }
        $this->options = new WriteOptions($codec, $dataPageSize, $dictionary, $dictionaryPageSize, $pageVersion, $statistics, $pageIndex);
        [$this->top, $this->leaves] = Compile::fields($schema instanceof Schema ? SchemaToType::fields($schema) : $schema);
        foreach ($this->leaves as $i => $_) {
            $this->buffers[$i] = new ChunkBuffer();
        }
        foreach ($this->top as $n) {
            $this->byName[$n->name] = $n;
        }
        // a schema of top-level scalars only takes a shorter path through add()
        $this->flat = true;
        foreach ($this->top as $n) {
            if (!$n->isLeaf()) {
                $this->flat = false;
                break;
            }
        }
        if ($this->flat) {
            foreach ($this->top as $i => $n) {
                $phys = $n->spec->phys;
                $this->fCols[$i] = [$n->name, $n->nullable, $n->spec->conv, $n->path, $n->maxDef > 0,
                    $phys === PhysicalType::ByteArray || $phys === PhysicalType::FixedLenByteArray, $n->spec->width === 0, $this->buffers[$n->leafIndex]];
            }
        }
    }

    /** The rows written so far. */
    public function rowCount(): int
    {
        return $this->totalRows + $this->rowsInGroup;
    }

    /**
     * Adds a row. Returns the bytes that are final now (the start of the file with the first call; a row group when one fills), else ''.
     *
     * @param array<string, mixed> $row
     * @throws \TypeError for a value of the wrong PHP type
     * @throws \ValueError for a value that does not fit (the row is refused whole, the writer stays usable)
     * @throws \Error if the writer is finished
     */
    public function encode(array $row): string
    {
        if ($this->closed) {
            throw new \Error('The writer is finished');
        }
        if ($this->flat) {
            $this->addFlat($row, $this->totalRows + $this->rowsInGroup);
        } else {
            $this->add($row);
        }
        return $this->rowsInGroup >= $this->rowGroupRows || $this->memory >= $this->rowGroupBytes ? $this->flushGroup() : $this->start();
    }

    /** The last row group, the page indexes and the footer. The writer is closed afterwards. */
    public function finish(): string
    {
        $this->assertOpen();
        $out = $this->start();
        if ($this->rowsInGroup > 0) {
            $out .= $this->flushGroup();
        }
        $this->closed = true;

        // the page indexes: all column indexes, then all offset indexes, after the last row group
        $colIdx = [];
        $offIdx = [];
        if ($this->options->pageIndex) {
            foreach ($this->groups as $g => $group) {
                foreach ($group['chunks'] as $c => $chunk) {
                    if ($chunk['columnIndex'] !== null) {
                        $colIdx[$g][$c] = [$this->pos, \strlen($chunk['columnIndex'])];
                        $out .= $chunk['columnIndex'];
                        $this->pos += \strlen($chunk['columnIndex']);
                    }
                }
            }
            foreach ($this->groups as $g => $group) {
                foreach ($group['chunks'] as $c => $chunk) {
                    $locs = [];
                    foreach ($chunk['pages'] as [$offset, $size, $firstRow]) {
                        $locs[] = [[1, T::I64, $offset], [2, T::I32, $size], [3, T::I64, $firstRow]];
                    }
                    $bytes = T::struct([[1, T::LIST, [T::STRUCT, $locs]]]);
                    $offIdx[$g][$c] = [$this->pos, \strlen($bytes)];
                    $out .= $bytes;
                    $this->pos += \strlen($bytes);
                }
            }
        }

        $rowGroups = [];
        foreach ($this->groups as $g => $group) {
            $chunks = [];
            $totalCompressed = 0;
            foreach ($group['chunks'] as $c => $chunk) {
                $leaf = $this->leaves[$c];
                $spec = $leaf->spec;
                $stats = null;
                if ($this->options->statistics) {
                    $stats = [[3, T::I64, $chunk['nullCount']]];
                    if ($chunk['stats'] !== null) {
                        $stats[] = [5, T::BINARY, Stats::raw($chunk['stats'][1], $spec->phys)];
                        $stats[] = [6, T::BINARY, Stats::raw($chunk['stats'][0], $spec->phys)];
                        $stats[] = [7, T::BOOL, true];
                        $stats[] = [8, T::BOOL, true];
                    }
                }
                $meta = [
                    [1, T::I32, $spec->phys->value],
                    [2, T::LIST, [T::I32, $chunk['encodings']]],
                    [3, T::LIST, [T::BINARY, $leaf->parts]],
                    [4, T::I32, $this->options->codec->value],
                    [5, T::I64, $chunk['numValues']],
                    [6, T::I64, $chunk['uncompressed']],
                    [7, T::I64, $chunk['compressed']],
                    [9, T::I64, $chunk['dataOffset']],
                    [11, T::I64, $chunk['dictOffset']],
                    [12, T::STRUCT, $stats],
                ];
                $chunks[] = [
                    [2, T::I64, $chunk['dictOffset'] ?? $chunk['dataOffset']],
                    [3, T::STRUCT, $meta],
                    [4, T::I64, $offIdx[$g][$c][0] ?? null],
                    [5, T::I32, $offIdx[$g][$c][1] ?? null],
                    [6, T::I64, $colIdx[$g][$c][0] ?? null],
                    [7, T::I32, $colIdx[$g][$c][1] ?? null],
                ];
                $totalCompressed += $chunk['compressed'];
            }
            $rowGroups[] = [
                [1, T::LIST, [T::STRUCT, $chunks]],
                [2, T::I64, $group['bytes']],
                [3, T::I64, $group['rows']],
                [5, T::I64, $group['start']],
                [6, T::I64, $totalCompressed],
                [7, T::I16, $g],
            ];
        }
        $kv = [];
        foreach ($this->keyValueMetadata as $k => $v) {
            $kv[] = [[1, T::BINARY, (string) $k], [2, T::BINARY, (string) $v]];
        }
        $footer = T::struct([
            [1, T::I32, 2],
            [2, T::LIST, [T::STRUCT, Compile::elements($this->top)]],
            [3, T::I64, $this->totalRows],
            [4, T::LIST, [T::STRUCT, $rowGroups]],
            [5, T::LIST, $kv === [] ? null : [T::STRUCT, $kv]],
            [6, T::BINARY, 'charm/parquet version ' . self::VERSION],
            [7, T::LIST, [T::STRUCT, \array_map(static fn (): array => [[1, T::STRUCT, []]], $this->leaves)]],
        ]);
        return $out . $footer . \pack('V', \strlen($footer)) . self::MAGIC;
    }

    private function assertOpen(): void
    {
        if ($this->closed) {
            throw new \Error('The writer is finished');
        }
    }

    /** The file magic, once. */
    private function start(): string
    {
        if ($this->started) {
            return '';
        }
        $this->started = true;
        $this->pos = \strlen(self::MAGIC);
        return self::MAGIC;
    }

    /** Validates, converts and buffers one row (all or nothing). */
    private function add(array $row): void
    {
        $rowNo = $this->totalRows + $this->rowsInGroup;
        foreach ($row as $k => $_) {
            if (!isset($this->byName[$k])) {
                throw new \ValueError("row $rowNo: unknown column '" . $k . "'");
            }
        }
        // phase 1: convert everything into staging arrays; nothing is buffered until the whole row is valid
        $flat = [];
        $stage = [];
        foreach ($this->top as $n) {
            $v = $row[$n->name] ?? null;
            try {
                if ($n->isLeaf()) {
                    if ($v === null) {
                        if (!$n->nullable) {
                            throw new \ValueError('null in a required column');
                        }
                        $flat[$n->leafIndex] = null;
                    } else {
                        $flat[$n->leafIndex] = ($n->spec->conv)($v);
                    }
                } else {
                    $this->shred($n, $v, 0, $stage);
                }
            } catch (\TypeError|\ValueError $e) {
                $msg = "column '{$n->path}', row $rowNo: " . $e->getMessage();
                throw $e instanceof \TypeError ? new \TypeError($msg, 0, $e) : new \ValueError($msg, 0, $e);
            }
        }
        // phase 2: commit
        foreach ($flat as $li => $c) {
            $b = $this->buffers[$li];
            $leaf = $this->leaves[$li];
            if ($leaf->maxDef > 0) {
                $b->defs[] = $c === null ? 0 : 1;
                $this->memory += 16;
            }
            if ($c !== null) {
                $b->vals[] = $c;
                $this->memory += \is_string($c) ? 48 + \strlen($c) : 16;
                if ($leaf->spec->width === 0) {
                    $b->plainBytes += 4 + \strlen($c);
                }
            }
        }
        foreach ($stage as $li => [$v, $d, $r]) {
            $b = $this->buffers[$li];
            $leaf = $this->leaves[$li];
            if ($v !== []) {
                \array_push($b->vals, ...$v);
                $variable = $leaf->spec->width === 0;
                foreach ($v as $x) {
                    if ($variable) {
                        $b->plainBytes += 4 + \strlen($x);
                    }
                    $this->memory += \is_string($x) ? 48 + \strlen($x) : 16;
                }
            }
            $m = 0;
            if ($leaf->maxDef > 0) {
                \array_push($b->defs, ...$d);
                $m = 16 * \count($d);
            }
            if ($leaf->maxRep > 0) {
                \array_push($b->reps, ...$r);
                $m += 16 * \count($r);
            }
            $this->memory += $m;
        }
        ++$this->rowsInGroup;
    }

    /** add() for a schema of top-level scalar columns: no staging arrays, no per-value estimate beyond what the type needs. */
    private function addFlat(array $row, int $rowNo): void
    {
        $conv = [];
        $present = 0;
        foreach ($this->fCols as $i => [$name, $nullable, $fn, $path]) {
            $v = $row[$name] ?? null;
            if ($v === null) {
                $present += \array_key_exists($name, $row) ? 1 : 0;
                if (!$nullable) {
                    throw new \ValueError("column '$path', row $rowNo: null in a required column");
                }
                $conv[$i] = null;
                continue;
            }
            ++$present;
            try {
                $conv[$i] = $fn($v);
            } catch (\TypeError|\ValueError $e) {
                $msg = "column '$path', row $rowNo: " . $e->getMessage();
                throw $e instanceof \TypeError ? new \TypeError($msg, 0, $e) : new \ValueError($msg, 0, $e);
            }
        }
        if ($present !== \count($row)) {
            foreach ($row as $k => $_) {
                if (!isset($this->byName[$k])) {
                    throw new \ValueError("row $rowNo: unknown column '" . $k . "'");
                }
            }
        }
        $mem = 0;
        foreach ($conv as $i => $c) {
            [, , , , $hasDef, $isString, $variable, $b] = $this->fCols[$i];
            if ($hasDef) {
                $b->defs[] = $c === null ? 0 : 1;
                $mem += 16;
            }
            if ($c !== null) {
                $b->vals[] = $c;
                if ($isString) {
                    $l = \strlen($c);
                    $mem += 48 + $l;
                    if ($variable) {
                        $b->plainBytes += 4 + $l;
                    }
                } else {
                    $mem += 16;
                }
            }
        }
        $this->memory += $mem;
        ++$this->rowsInGroup;
    }

    /**
     * Shreds one value of a nested node into the staging arrays [values, definition levels, repetition levels] of its leaves.
     *
     * @param array<int, array{list<mixed>, list<int>, list<int>}> $stage
     */
    private function shred(WNode $n, mixed $v, int $rep, array &$stage): void
    {
        if ($v === null) {
            if (!$n->nullable) {
                throw new \ValueError("null in a required field '{$n->path}'");
            }
            $this->emit($n, $rep, $n->maxDef - 1, $stage);
            return;
        }
        switch ($n->kind) {
            case 'struct':
                if (!\is_array($v)) {
                    throw new \TypeError("expected an array for the struct '{$n->path}', got " . \get_debug_type($v));
                }
                foreach ($v as $k => $_) {
                    if (!\in_array((string) $k, $n->names, true)) {
                        throw new \ValueError("unknown field '$k' in the struct '{$n->path}'");
                    }
                }
                foreach ($n->children as $i => $c) {
                    $this->shred($c, $v[$n->names[$i]] ?? null, $rep, $stage);
                }
                return;
            case 'list':
                if (!\is_array($v)) {
                    throw new \TypeError("expected an array for the list '{$n->path}', got " . \get_debug_type($v));
                }
                if (!\array_is_list($v)) {
                    throw new \ValueError("the list '{$n->path}' must be a PHP list (sequential keys)");
                }
                if ($v === []) {
                    $this->emit($n, $rep, $n->maxDef, $stage);
                    return;
                }
                foreach ($v as $i => $el) {
                    $this->shred($n->children[0], $el, $i === 0 ? $rep : $n->repLevel, $stage);
                }
                return;
            case 'map':
                if (!\is_array($v)) {
                    throw new \TypeError("expected an array for the map '{$n->path}', got " . \get_debug_type($v));
                }
                if ($v === []) {
                    $this->emit($n, $rep, $n->maxDef, $stage);
                    return;
                }
                $i = 0;
                foreach (self::entries($n, $v) as [$key, $val]) {
                    $r = $i++ === 0 ? $rep : $n->repLevel;
                    $this->shred($n->children[0], $key, $r, $stage);
                    $this->shred($n->children[1], $val, $r, $stage);
                }
                return;
        }
        // a leaf
        $c = ($n->spec->conv)($v);
        $li = $n->leafIndex;
        $stage[$li] ??= [[], [], []];
        $stage[$li][0][] = $c;
        $stage[$li][1][] = $n->maxDef;
        $stage[$li][2][] = $rep;
    }

    /** An entry per leaf below $n without a value: the same definition and repetition level for each. @param array<int, array{list<mixed>, list<int>, list<int>}> $stage */
    private function emit(WNode $n, int $rep, int $def, array &$stage): void
    {
        foreach ($n->leaves as $l) {
            $li = $l->leafIndex;
            $stage[$li] ??= [[], [], []];
            $stage[$li][1][] = $def;
            $stage[$li][2][] = $rep;
        }
    }

    /**
     * The [key, value] pairs of a map value: an associative array (an int key is turned into a string for a key column that is not an
     * integer, as PHP made it one) or a list of [key, value] pairs.
     *
     * @return list<array{mixed, mixed}>
     */
    private static function entries(WNode $map, array $v): array
    {
        $keyKind = $map->children[0]->kind;
        $valueIsArray = \in_array($map->children[1]->kind, ['list', 'map', 'struct'], true);
        $pairs = \array_is_list($v);
        if ($pairs) {
            foreach ($v as $e) {
                if (!\is_array($e) || \count($e) !== 2 || !\array_is_list($e)) {
                    $pairs = false;
                    break;
                }
                // a list of 2-element arrays is a list of pairs, unless the values are arrays and the first item is not a scalar key
                if ($valueIsArray && \is_array($e[0])) {
                    $pairs = false;
                    break;
                }
            }
        }
        if ($pairs) {
            return $v;
        }
        $intKey = \in_array($keyKind, ['int8', 'int16', 'int32', 'int64', 'uint8', 'uint16', 'uint32', 'uint64', 'date', 'time', 'timestamp'], true);
        $out = [];
        foreach ($v as $k => $val) {
            $out[] = [\is_int($k) && !$intKey ? (string) $k : $k, $val];
        }
        return $out;
    }

    private function flushGroup(): string
    {
        $out = $this->start();
        $chunks = [];
        $start = $this->pos;
        $bytes = 0;
        foreach ($this->leaves as $li => $leaf) {
            $r = ChunkEncoder::encode($this->buffers[$li], $leaf, $this->options, $this->pos);
            $this->buffers[$li]->clear();
            $out .= $r['bytes'];
            $this->pos += \strlen($r['bytes']);
            $bytes += $r['uncompressed'];
            $r['bytes'] = '';
            $chunks[] = $r;
        }
        $this->groups[] = ['rows' => $this->rowsInGroup, 'bytes' => $bytes, 'start' => $start, 'chunks' => $chunks];
        $this->totalRows += $this->rowsInGroup;
        $this->rowsInGroup = 0;
        $this->memory = 0;
        return $out;
    }
}
