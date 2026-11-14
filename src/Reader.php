<?php

declare(strict_types=1);

namespace Charm\Parquet;

use Charm\Parquet\Internal\{Assembler, ColumnReader, FileSource, IndexParser, LeafData, Logical, MetadataParser, RangeCompare, Ranges, Shape};

/**
 * Reads a Parquet file through a range function: `function (int $offset, int $length): string` returning exactly $length bytes
 * (the reader never asks for bytes beyond the file size it is given). The package does no I/O of its own beyond fromFile().
 *
 * The footer is read when the Reader is constructed: one read of the last 64 KiB, and a second one only if the footer is larger.
 * Columns are read one column chunk at a time (one range read each; a chunk is held in memory whole while it is decoded).
 *
 * A column is addressed by its dotted path in the real schema names (`a.list.element`, `m.key_value.key`). rows() yields rows keyed by
 * top-level field name with structs as associative arrays, lists as lists and maps as {@see MapMode} says; readColumn() takes the path
 * of a leaf (its projected shape) or of a group (the assembled value); readShredded() gives one leaf chunk as values and levels.
 */
final class Reader
{
    private const TAIL = 65536;
    private const SLACK = 256;

    private readonly FileMetadata $metadata;
    private int $reads = 0;
    private ?FileSource $source = null;
    /** @var array<int, (\Closure(array): array)|false> */
    private array $converters = [];
    /** @var array<int, list<array{int, int, string}>> index bytes already read, per row group: [start, end, bytes] */
    private array $indexBlocks = [];
    /** @var array<string, OffsetIndex|false> */
    private array $offsetIndexes = [];
    /** @var array<string, ColumnIndex|false> */
    private array $columnIndexes = [];
    /** @var array<int, int> row group index by spl_object_id */
    private array $groupIndex = [];

    /**
     * @param \Closure(int, int): string $read exceptions it throws pass through unchanged; a return value that is not exactly $length bytes is an \UnexpectedValueException
     * @param int $size the size of the file in bytes
     * @param int $maxPageSize the largest uncompressed page accepted (UnsupportedException above it)
     * @param int $maxChunkValues the most values (level entries; rows for a flat column) in one column chunk accepted, which bounds the memory a column needs
     * @param MapMode $mapMode how maps are returned
     */
    public function __construct(
        private readonly \Closure $read,
        private readonly int $size,
        private readonly ValueMode $values = ValueMode::Logical,
        private readonly int $maxPageSize = 1 << 30,
        private readonly int $maxChunkValues = 1 << 25,
        private readonly MapMode $mapMode = MapMode::Auto,
    ) {
        if (\PHP_INT_SIZE < 8) {
            throw new \Error('64-bit PHP (PHP_INT_SIZE 8) is required: INT64 values and sizes are read as PHP ints');
        }
        $this->metadata = $this->readFooter();
        foreach ($this->metadata->rowGroups as $i => $g) {
            $this->groupIndex[\spl_object_id($g)] = $i;
        }
    }

    /**
     * @throws \RuntimeException if the file cannot be opened or read
     * @throws ParquetException if it is not a valid Parquet file
     * @throws UnsupportedException if it uses something this version does not implement
     */
    public static function fromFile(string $path, ValueMode $values = ValueMode::Logical, int $maxPageSize = 1 << 30, int $maxChunkValues = 1 << 25, MapMode $mapMode = MapMode::Auto): self
    {
        $source = new FileSource($path);
        $reader = new self($source->read(...), $source->size(), $values, $maxPageSize, $maxChunkValues, $mapMode);
        $reader->source = $source; // (keeps the file handle alive as long as the reader)
        return $reader;
    }

    public static function fromString(string $bytes, ValueMode $values = ValueMode::Logical, int $maxPageSize = 1 << 30, int $maxChunkValues = 1 << 25, MapMode $mapMode = MapMode::Auto): self
    {
        return new self(static fn (int $offset, int $length): string => \substr($bytes, $offset, $length), \strlen($bytes), $values, $maxPageSize, $maxChunkValues, $mapMode);
    }

    public function metadata(): FileMetadata
    {
        return $this->metadata;
    }

    public function schema(): Schema
    {
        return $this->metadata->schema;
    }

    /**
     * The file's num_rows as the footer states it. Some writers get it wrong; {@see self::rowCount()} is the sum over the row groups.
     */
    public function numRows(): int
    {
        return $this->metadata->numRows;
    }

    /** The sum of the row counts of the row groups, which is what rows() yields. */
    public function rowCount(): int
    {
        return $this->metadata->rowGroupRowCount();
    }

    public function rowGroupCount(): int
    {
        return \count($this->metadata->rowGroups);
    }

    /** How many times the range function has been called so far. */
    public function readCalls(): int
    {
        return $this->reads;
    }

    /**
     * The values of one column in one row group, one entry per row of $offset .. $offset + $limit (relative to the row group; default
     * all rows).
     *
     * A leaf path gives its projected shape: the list levels on the way are kept, wrapper structs are dropped (a leaf `a.list.element.b`
     * in `a: list<struct<b, c>>` gives, per row, a list of b values, with null where the list or the struct is null). A group path
     * gives the assembled value: an associative array for a struct, a list for a list, a map as {@see MapMode} says.
     *
     * @return list<mixed>
     */
    public function readColumn(int $rowGroup, string $column, int $offset = 0, ?int $limit = null): array
    {
        $rg = $this->metadata->rowGroups[$rowGroup] ?? throw new \ValueError("There is no row group $rowGroup");
        $node = $this->metadata->schema->node($column) ?? throw new \ValueError("There is no column '$column'");
        return $this->columnOf($rg, $node, $this->groupRange($rg, $offset, $limit));
    }

    /**
     * Several columns of one row group, as path => one entry per row of $offset .. $offset + $limit. The default is every top-level field.
     *
     * @param list<string>|null $columns dotted paths of leaves or groups
     * @return array<string, list<mixed>>
     */
    public function readRowGroup(int $rowGroup, ?array $columns = null, int $offset = 0, ?int $limit = null): array
    {
        $rg = $this->metadata->rowGroups[$rowGroup] ?? throw new \ValueError("There is no row group $rowGroup");
        $range = $this->groupRange($rg, $offset, $limit);
        $out = [];
        foreach ($columns ?? \array_map(static fn (SchemaNode $n): string => $n->name, $this->metadata->schema->root->children) as $path) {
            $node = $this->metadata->schema->node($path) ?? throw new \ValueError("There is no column '$path'");
            $out[$path] = $this->columnOf($rg, $node, $range);
        }
        return $out;
    }

    /** @return array{int, int}|null [first row, row count] inside the row group, or null for all of it */
    private function groupRange(RowGroup $rg, int $offset, ?int $limit): ?array
    {
        if ($offset < 0 || ($limit !== null && $limit < 0)) {
            throw new \ValueError('offset and limit must not be negative');
        }
        $start = \min($offset, $rg->numRows);
        $count = \min($limit ?? $rg->numRows, $rg->numRows - $start);
        return $start === 0 && $count === $rg->numRows ? null : [$start, $count];
    }

    /**
     * The rows, one row group at a time (only one row group is in memory): each row is [top-level field name => assembled value].
     * A path in $columns may name a leaf or a group; a group selects every leaf below it, and some leaves of a struct give a partial
     * struct (only those fields).
     *
     * Exactly the rows $offset .. $offset + $limit of the file are yielded (all by default); with $ranges (a list of [first row, row
     * count] pairs, such as candidateRanges() returns) only the rows of the file that lie in a range and in that window. Row groups with
     * no wanted row are never read; inside a row group, pages that hold no wanted row are not read when the column has an offset index.
     *
     * @param list<string>|null $columns default: every top-level field
     * @param list<array{int, int}>|null $ranges
     * @return \Generator<int, array<string, mixed>>
     */
    public function rows(?array $columns = null, int $offset = 0, ?int $limit = null, ?array $ranges = null): \Generator
    {
        if ($offset < 0 || ($limit !== null && $limit < 0)) {
            throw new \ValueError('offset and limit must not be negative');
        }
        $tops = $this->selection($columns);
        $want = $ranges === null ? [[0, \PHP_INT_MAX]] : Ranges::fromCounts($ranges);
        $want = Ranges::within($want, $offset, $limit === null || $limit > \PHP_INT_MAX - $offset ? \PHP_INT_MAX : $offset + $limit);
        $want = \array_map(static fn (array $r): array => [$r[0] + $offset, $r[1] + $offset], $want);
        $base = 0;
        foreach ($this->metadata->rowGroups as $rg) {
            $lo = $base;
            $base += $rg->numRows;
            $local = Ranges::within($want, $lo, $base);
            if ($local === []) {
                if ($want !== [] && $want[\count($want) - 1][1] <= $lo) {
                    break; // (everything wanted lies before this row group)
                }
                continue;
            }
            $s = $local[0][0];
            $e = $local[\count($local) - 1][1];
            $range = $s === 0 && $e === $rg->numRows ? null : [$s, $e - $s];
            $cols = [];
            foreach ($tops as $name => [$top, $sel]) {
                $cols[$name] = $this->topColumn($rg, $top, $sel, $range);
            }
            $names = \array_keys($cols);
            $lists = \array_values($cols);
            foreach ($lists as $i => $vals) {
                if (\count($vals) !== $e - $s) {
                    throw new ParquetException("Field '{$names[$i]}' has " . \count($vals) . ' rows in a row group read of ' . ($e - $s));
                }
            }
            foreach ($local as [$a, $b]) {
                for ($r = $a; $r < $b; $r++) {
                    // (one row = the r-th entry of every column: array_column and array_combine do it in C, which beats a loop over the columns)
                    yield \array_combine($names, \array_column($lists, $r - $s));
                }
            }
            unset($cols, $lists);
        }
    }

    /**
     * One column chunk in its shredded form: the non-null values and the definition and repetition levels of every entry.
     */
    public function readShredded(int $rowGroup, string $column): ShreddedColumn
    {
        $rg = $this->metadata->rowGroups[$rowGroup] ?? throw new \ValueError("There is no row group $rowGroup");
        $leaf = $this->metadata->schema->leaf($column) ?? throw new \ValueError("There is no column '$column'");
        return $this->shred($rg, $leaf);
    }

    /**
     * @param list<string>|null $columns
     * @return array<string, array{SchemaNode, array<int, true>|null}> top-level name => [node, selected leaf indexes (null: all)]
     */
    private function selection(?array $columns): array
    {
        $schema = $this->metadata->schema;
        if ($columns === null) {
            $out = [];
            foreach ($schema->root->children as $top) {
                $out[$top->name] = [$top, null];
            }
            return $out;
        }
        $sel = [];
        $tops = [];
        foreach ($columns as $path) {
            $node = $schema->node($path) ?? throw new \ValueError("There is no column '$path'");
            $top = $schema->node($node->path[0]);
            $tops[$top->name] ??= $top;
            foreach (self::leavesUnder($node) as $l) {
                $sel[$top->name][$l->leafIndex] = true;
            }
        }
        $out = [];
        foreach ($tops as $name => $top) {
            $all = \count(self::leavesUnder($top)) === \count($sel[$name]);
            $out[$name] = [$top, $all ? null : $sel[$name]];
        }
        return $out;
    }

    /** @return list<SchemaNode> */
    private static function leavesUnder(SchemaNode $n): array
    {
        if ($n->isLeaf()) {
            return [$n];
        }
        $out = [];
        foreach ($n->children as $c) {
            foreach (self::leavesUnder($c) as $l) {
                $out[] = $l;
            }
        }
        return $out;
    }

    /** The value of a top-level field for every row. @param array<int, true>|null $sel */
    private function topColumn(RowGroup $rg, SchemaNode $top, ?array $sel, ?array $range): array
    {
        if ($top->isLeaf() && $top->isFlat() && $range === null) {
            return $this->readChunk($rg, $top);
        }
        return $this->assemble($rg, $top, $sel, $range);
    }

    /** The column of a leaf (projected) or of a group (assembled), wrapped in the lists above it: one entry per row. */
    private function columnOf(RowGroup $rg, SchemaNode $node, ?array $range = null): array
    {
        if ($range !== null && $range[1] === 0) {
            return [];
        }
        if ($node->isLeaf()) {
            if ($node->isFlat() && $range === null) {
                return $this->readChunk($rg, $node);
            }
            $d = $this->leafData($rg, $node, $range);
            return Assembler::wrap(Assembler::leafColumn($node, $d->values, $d->slot[$d->m], $d->m), $d->cnt, $d->m);
        }
        $sel = [];
        foreach (self::leavesUnder($node) as $l) {
            $sel[$l->leafIndex] = true;
        }
        return $this->assemble($rg, $node, $sel, $range);
    }

    /**
     * Assembles the field $node (a top-level field, or a group deeper down) over the selected leaves.
     *
     * @param array<int, true>|null $sel
     */
    private function assemble(RowGroup $rg, SchemaNode $node, ?array $sel, ?array $range = null): array
    {
        $shape = Shape::field($node, $sel, $this->mapMode, $this->values) ?? throw new \ValueError("Nothing is selected below '{$node->dottedPath}'");
        $data = [];
        foreach ($shape->leaves as $leaf) {
            $data[$leaf->leafIndex] = $this->leafData($rg, $leaf, $range);
        }
        $j0 = $node->maxRepetitionLevel - ($node->repetition === Repetition::Repeated ? 1 : 0);
        $col = Assembler::build($shape, $data, $j0);
        $drv = $data[$shape->leaves[0]->leafIndex];
        $col = Assembler::wrap($col, $drv->cnt, $j0);
        $expected = $range === null ? $rg->numRows : $range[1];
        if (\count($col) !== $expected) {
            throw new ParquetException("'{$node->dottedPath}' assembled to " . \count($col) . " rows, expected $expected");
        }
        return $col;
    }

    private function leafData(RowGroup $rg, SchemaNode $leaf, ?array $range = null): LeafData
    {
        $sc = $this->shred($rg, $leaf, $range);
        $dR = $this->repeatedLevels($leaf);
        [$slot, $cnt] = Assembler::slots($sc, $dR);
        return new LeafData($leaf, $sc->values, $slot, $cnt, $leaf->maxRepetitionLevel);
    }

    /** @return list<int> the definition level of each repeated node on the way down to $leaf, outermost first */
    private function repeatedLevels(SchemaNode $leaf): array
    {
        $out = [];
        $n = $this->metadata->schema->root;
        foreach ($leaf->path as $name) {
            foreach ($n->children as $c) {
                if ($c->name === $name) {
                    $n = $c;
                    break;
                }
            }
            if ($n->repetition === Repetition::Repeated) {
                $out[] = $n->maxDefinitionLevel;
            }
        }
        return $out;
    }

    /** Validates a column chunk (throws UnsupportedException or ParquetException) and returns it. */
    private function checkedChunk(RowGroup $rg, SchemaNode $leaf): ColumnChunk
    {
        $meta = $rg->columns[$leaf->leafIndex ?? 0];
        if ($meta->unsupported !== null) {
            throw new UnsupportedException($meta->unsupported);
        }
        if ($meta->encrypted) {
            throw new UnsupportedException("encrypted column {$leaf->dottedPath}");
        }
        if ($meta->filePath !== null && $meta->filePath !== '') {
            throw new UnsupportedException("column {$leaf->dottedPath} stored in another file ('{$meta->filePath}')");
        }
        if ($rg->numRows > $this->maxChunkValues || $meta->numValues > $this->maxChunkValues) {
            throw new UnsupportedException("column chunk of {$meta->numValues} values ({$rg->numRows} rows) above the limit of {$this->maxChunkValues}");
        }
        $start = $meta->startOffset();
        $len = $meta->totalCompressedSize;
        if ($start < 0 || $len > $this->size || $start > $this->size - $len) {
            throw new ParquetException("Column '{$leaf->dottedPath}' lies outside the file");
        }
        return $meta;
    }

    /** Validates a column chunk and reads all of its bytes (one range call). @return array{ColumnChunk, string} */
    private function chunkBytes(RowGroup $rg, SchemaNode $leaf): array
    {
        $meta = $this->checkedChunk($rg, $leaf);
        // A few bytes more than the footer says: some writers leave the dictionary page header out of total_compressed_size.
        return [$meta, $this->range($meta->startOffset(), \min($meta->totalCompressedSize + self::SLACK, $this->size - $meta->startOffset()))];
    }

    /**
     * One leaf chunk in shredded form: all of it, or exactly the rows $range = [first, count] (read from the pages that hold them when
     * the column has an offset index, else from the whole chunk).
     */
    private function shred(RowGroup $rg, SchemaNode $leaf, ?array $range = null): ShreddedColumn
    {
        $meta = $rg->columns[$leaf->leafIndex ?? 0];
        if ($rg->numRows === 0 && $meta->numValues === 0 && $meta->unsupported === null && !$meta->encrypted) {
            return new ShreddedColumn($leaf, [], $leaf->maxDefinitionLevel > 0 ? [] : null, $leaf->maxRepetitionLevel > 0 ? [] : null);
        }
        if ($range !== null && $range[0] === 0 && $range[1] === $rg->numRows) {
            $range = null;
        }
        if ($range === null) {
            [$meta, $bytes] = $this->chunkBytes($rg, $leaf);
            [$vals, $defs, $reps] = ColumnReader::shred($bytes, $meta, $rg->numRows, $this->maxPageSize);
            return new ShreddedColumn($leaf, $this->convert($vals, $leaf), $defs, $reps);
        }
        [$from, $count] = $range;
        $meta = $this->checkedChunk($rg, $leaf);
        $oi = $this->offsetIndexOf($rg, $leaf);
        if ($oi === null || $oi->pages === []) {
            [$meta, $bytes] = $this->chunkBytes($rg, $leaf);
            [$vals, $defs, $reps] = ColumnReader::shred($bytes, $meta, $rg->numRows, $this->maxPageSize);
            $first = 0;
        } else {
            $pages = $oi->pages;
            $np = \count($pages);
            $i0 = 0;
            while ($i0 + 1 < $np && $pages[$i0 + 1]->firstRowIndex <= $from) {
                ++$i0;
            }
            while ($i0 > 0 && $pages[$i0 - 1]->firstRowIndex === $pages[$i0]->firstRowIndex) {
                --$i0; // (start at the page that starts the row, not at one that continues it)
            }
            $i1 = $i0;
            while ($i1 + 1 < $np && $pages[$i1 + 1]->firstRowIndex < $from + $count) {
                ++$i1;
            }
            $first = $pages[$i0]->firstRowIndex;
            $covered = $oi->endRow($i1, $rg->numRows) - $first;
            // the dictionary page (everything between the chunk start and the first data page) and the selected pages, adjacent ones in one read
            $wanted = [];
            $dictEnd = $pages[0]->offset;
            if ($dictEnd > $meta->startOffset()) {
                $wanted[] = [$meta->startOffset(), $dictEnd];
            }
            for ($i = $i0; $i <= $i1; $i++) {
                $wanted[] = [$pages[$i]->offset, $pages[$i]->offset + $pages[$i]->compressedSize];
            }
            $bytes = '';
            $n = \count($wanted);
            for ($k = 0; $k < $n;) {
                $s = $wanted[$k][0];
                $e = $wanted[$k][1];
                for ($k++; $k < $n && $wanted[$k][0] === $e; $k++) {
                    $e = $wanted[$k][1];
                }
                $bytes .= $this->range($s, $e - $s);
            }
            [$vals, $defs, $reps] = ColumnReader::shredPages($bytes, $meta, $covered, $this->maxPageSize);
        }
        [$vals, $defs, $reps] = self::trim($vals, $defs, $reps, $leaf->maxDefinitionLevel, $range[0] - $first, $count);
        return new ShreddedColumn($leaf, $this->convert($vals, $leaf), $defs, $reps);
    }

    /**
     * The rows $from .. $from + $count of shredded data: the entries of those records, and the values that belong to them.
     *
     * @param list<mixed> $vals
     * @param list<int>|null $defs
     * @param list<int>|null $reps
     * @return array{list<mixed>, list<int>|null, list<int>|null}
     */
    private static function trim(array $vals, ?array $defs, ?array $reps, int $maxDef, int $from, int $count): array
    {
        if ($reps === null) {
            $e0 = $from;
            $e1 = $from + $count;
        } else {
            $starts = \array_keys($reps, 0, true);
            if ($from + $count > \count($starts)) {
                throw new ParquetException('The pages hold fewer rows than the offset index says');
            }
            $e0 = $starts[$from];
            $e1 = $starts[$from + $count] ?? \count($reps);
        }
        if ($defs === null) {
            return [\array_slice($vals, $e0, $e1 - $e0), null, $reps === null ? null : \array_slice($reps, $e0, $e1 - $e0)];
        }
        $before = $e0 === 0 ? 0 : (\array_count_values(\array_slice($defs, 0, $e0))[$maxDef] ?? 0);
        $kept = \array_slice($defs, $e0, $e1 - $e0);
        $present = \array_count_values($kept)[$maxDef] ?? 0;
        return [\array_slice($vals, $before, $present), $kept, $reps === null ? null : \array_slice($reps, $e0, $e1 - $e0)];
    }

    /** A flat column: one entry per row. */
    private function readChunk(RowGroup $rg, SchemaNode $leaf): array
    {
        $meta = $rg->columns[$leaf->leafIndex ?? 0];
        if ($meta->unsupported !== null || $meta->encrypted || ($meta->filePath !== null && $meta->filePath !== '') || $rg->numRows > $this->maxChunkValues) {
            $this->chunkBytes($rg, $leaf); // (throws the right exception)
        }
        if ($meta->numValues !== $rg->numRows) {
            throw new ParquetException("Column '{$leaf->dottedPath}' has {$meta->numValues} values in a row group of {$rg->numRows} rows");
        }
        if ($rg->numRows === 0) {
            return [];
        }
        [$meta, $bytes] = $this->chunkBytes($rg, $leaf);
        $values = ColumnReader::read($bytes, $meta, $rg->numRows, $this->maxPageSize);
        return $this->convert($values, $leaf);
    }

    // ---- page indexes ----

    /**
     * The page locations of a column chunk (offset, size, first row), or null if the file has no offset index for it. The index is read
     * on first use (one range call, shared with the other indexes of the row group when they are adjacent) and validated against the chunk.
     */
    public function offsetIndex(int $rowGroup, string $column): ?OffsetIndex
    {
        $rg = $this->metadata->rowGroups[$rowGroup] ?? throw new \ValueError("There is no row group $rowGroup");
        $leaf = $this->metadata->schema->leaf($column) ?? throw new \ValueError("There is no column '$column'");
        return $this->offsetIndexOf($rg, $leaf);
    }

    /**
     * The per-page statistics of a column chunk, or null if the file has no column index for it. min and max are in this Reader's value mode.
     */
    public function columnIndex(int $rowGroup, string $column): ?ColumnIndex
    {
        $rg = $this->metadata->rowGroups[$rowGroup] ?? throw new \ValueError("There is no row group $rowGroup");
        $leaf = $this->metadata->schema->leaf($column) ?? throw new \ValueError("There is no column '$column'");
        return $this->columnIndexOf($rg, $leaf);
    }

    private function offsetIndexOf(RowGroup $rg, SchemaNode $leaf): ?OffsetIndex
    {
        $meta = $rg->columns[$leaf->leafIndex ?? 0];
        $key = $this->groupIndex[\spl_object_id($rg)] . ':' . $leaf->leafIndex;
        if (isset($this->offsetIndexes[$key])) {
            return $this->offsetIndexes[$key] ?: null;
        }
        $o = $meta->offsetIndexOffset;
        $l = $meta->offsetIndexLength;
        if ($meta->unsupported !== null || $o === null || $l === null || $o <= 0 || $l <= 0) {
            $this->offsetIndexes[$key] = false;
            return null;
        }
        $oi = IndexParser::offsetIndex($this->indexBytes($rg, $o, $l, $leaf));
        $this->validateOffsetIndex($oi, $meta, $rg, $leaf);
        $this->offsetIndexes[$key] = $oi;
        return $oi;
    }

    private function columnIndexOf(RowGroup $rg, SchemaNode $leaf): ?ColumnIndex
    {
        $meta = $rg->columns[$leaf->leafIndex ?? 0];
        $key = $this->groupIndex[\spl_object_id($rg)] . ':' . $leaf->leafIndex;
        if (isset($this->columnIndexes[$key])) {
            return $this->columnIndexes[$key] ?: null;
        }
        $o = $meta->columnIndexOffset;
        $l = $meta->columnIndexLength;
        if ($meta->unsupported !== null || $o === null || $l === null || $o <= 0 || $l <= 0) {
            $this->columnIndexes[$key] = false;
            return null;
        }
        $ci = IndexParser::columnIndex($this->indexBytes($rg, $o, $l, $leaf), fn (?string $raw) => $this->statValue($leaf, $raw));
        $this->columnIndexes[$key] = $ci;
        return $ci;
    }

    /** The bytes of an index. All index ranges of the row group that are adjacent to the wanted one come with it in the same read. */
    private function indexBytes(RowGroup $rg, int $offset, int $length, SchemaNode $leaf): string
    {
        if ($length > $this->maxPageSize || $offset < 4 || $offset > $this->size - $length) {
            throw new ParquetException("The page index of column '{$leaf->dottedPath}' lies outside the file or is too large");
        }
        $g = $this->groupIndex[\spl_object_id($rg)];
        foreach ($this->indexBlocks[$g] ?? [] as [$s, $e, $bytes]) {
            if ($offset >= $s && $offset + $length <= $e) {
                return \substr($bytes, $offset - $s, $length);
            }
        }
        $ranges = [[$offset, $offset + $length]];
        foreach ($rg->columns as $c) {
            foreach ([[$c->offsetIndexOffset, $c->offsetIndexLength], [$c->columnIndexOffset, $c->columnIndexLength]] as [$o, $l]) {
                if ($o !== null && $l !== null && $o >= 4 && $l > 0 && $l <= $this->maxPageSize && $o <= $this->size - $l) {
                    $ranges[] = [$o, $o + $l];
                }
            }
        }
        foreach (Ranges::merge($ranges) as [$s, $e]) {
            if ($offset >= $s && $offset + $length <= $e) {
                $bytes = $this->range($s, $e - $s);
                $this->indexBlocks[$g][] = [$s, $e, $bytes];
                return \substr($bytes, $offset - $s, $length);
            }
        }
        throw new \Error('unreachable');
    }

    private function validateOffsetIndex(OffsetIndex $oi, ColumnChunk $meta, RowGroup $rg, SchemaNode $leaf): void
    {
        $name = $leaf->dottedPath;
        $pages = $oi->pages;
        if ($rg->numRows === 0) {
            return;
        }
        if ($pages === []) {
            throw new ParquetException("The offset index of column '$name' lists no pages for a row group of {$rg->numRows} rows");
        }
        $chunkStart = $meta->startOffset();
        $chunkEnd = \min($this->size, $chunkStart + $meta->totalCompressedSize + self::SLACK);
        $prevEnd = $chunkStart;
        $prevRow = -1;
        foreach ($pages as $i => $p) {
            if ($p->compressedSize <= 0 || $p->offset < $prevEnd || $p->offset + $p->compressedSize > $chunkEnd) {
                throw new ParquetException("Page $i of column '$name' lies outside its column chunk, or overlaps the page before it");
            }
            // (a page of a repeated column may continue the row the previous page started: the same first row index, never a smaller one)
            if ($p->firstRowIndex < $prevRow || ($p->firstRowIndex === $prevRow && $leaf->maxRepetitionLevel === 0) || $p->firstRowIndex >= $rg->numRows || ($i === 0 && $p->firstRowIndex !== 0)) {
                throw new ParquetException("The first row indexes of the pages of column '$name' are not increasing from 0 within the row group");
            }
            $prevEnd = $p->offset + $p->compressedSize;
            $prevRow = $p->firstRowIndex;
        }
    }

    // ---- candidate row ranges ----

    /**
     * Which rows could hold a value of $column between $min and $max (inclusive; null: no bound on that side), or, with $orNull, a null.
     * Returns [first row, row count] ranges in file order (adjacent ones merged), from the row group statistics and, when the file has
     * them, the column index and offset index of each row group.
     *
     * It is conservative: a row that could match is never left out. Missing statistics, a statistic flagged inexact, NaN, values of a
     * kind that cannot be compared safely, and every column that is not a plain value per row where nulls matter, all mean "keep".
     * It does no per-value filtering: the caller filters the rows it reads. $min and $max are in this Reader's value mode (ints for an
     * INT column, "YYYY-MM-DD" for a DATE in logical mode, a decimal string for a DECIMAL, bytes for a string column).
     *
     * @return list<array{int, int}> feed it to rows($columns, ranges: ...)
     */
    public function candidateRanges(string $column, mixed $min = null, mixed $max = null, bool $orNull = false): array
    {
        $leaf = $this->metadata->schema->leaf($column) ?? throw new \ValueError("There is no column '$column'");
        $out = [];
        $base = 0;
        foreach ($this->metadata->rowGroups as $g => $rg) {
            $n = $rg->numRows;
            $first = $base;
            $base += $n;
            if ($n === 0 || $leaf->leafIndex === null) {
                continue;
            }
            $stats = $this->statistics($g, $column);
            if ($stats !== null && !$this->mayMatch($leaf, $stats->min, $stats->max, $stats->isMinExact, $stats->isMaxExact, $stats->nullCount, $min, $max, $orNull)) {
                continue;
            }
            $ci = $this->columnIndexOf($rg, $leaf);
            $oi = $ci === null ? null : $this->offsetIndexOf($rg, $leaf);
            if ($ci === null || $oi === null || \count($oi->pages) !== \count($ci->nullPages)) {
                $out[] = [$first, $n];
                continue;
            }
            foreach ($oi->pages as $i => $page) {
                $keep = $ci->nullPages[$i]
                    ? $orNull
                    : $this->mayMatch($leaf, $ci->min[$i], $ci->max[$i], null, null, $ci->nullCounts[$i] ?? null, $min, $max, $orNull);
                if ($keep) {
                    $out[] = [$first + $page->firstRowIndex, $oi->endRow($i, $n) - $page->firstRowIndex];
                }
            }
        }
        // merge ranges that touch or overlap
        $ends = Ranges::merge(\array_map(static fn (array $r): array => [$r[0], $r[0] + $r[1]], $out));
        return \array_map(static fn (array $r): array => [$r[0], $r[1] - $r[0]], $ends);
    }

    /** Could a chunk or page with these statistics hold a value in [$qMin, $qMax] (or a null, with $orNull)? True when unsure. */
    private function mayMatch(SchemaNode $leaf, mixed $sMin, mixed $sMax, ?bool $minExact, ?bool $maxExact, ?int $nullCount, mixed $qMin, mixed $qMax, bool $orNull): bool
    {
        if ($orNull && $leaf->maxDefinitionLevel > 0 && ($leaf->maxRepetitionLevel > 0 || $leaf->maxDefinitionLevel > 1 || $nullCount === null || $nullCount > 0)) {
            return true; // (a null could be here; in nested columns a null value is not the same as a null row)
        }
        if ($qMin === null && $qMax === null) {
            return true;
        }
        // no value can match if the whole [sMin, sMax] lies outside the query; an inexact or missing bound proves nothing
        if ($qMax !== null && $sMin !== null && $minExact !== false && RangeCompare::compare($sMin, $qMax, $leaf, $this->values) > 0) {
            return false;
        }
        if ($qMin !== null && $sMax !== null && $maxExact !== false && RangeCompare::compare($sMax, $qMin, $leaf, $this->values) < 0) {
            return false;
        }
        return true;
    }

    /** Logical-type conversion (decimals, dates, timestamps, ...), chosen once per column; physical mode passes values through. */
    private function convert(array $values, SchemaNode $leaf): array
    {
        if ($this->values === ValueMode::Physical) {
            return $values;
        }
        $conv = $this->converters[$leaf->leafIndex ?? 0] ??= Logical::converter($leaf) ?? false;
        return $conv === false ? $values : $conv($values);
    }

    /**
     * The decoded statistics of a column chunk, or null if the footer has none. Values are in this Reader's value mode.
     */
    public function statistics(int $rowGroup, string $column): ?ColumnStatistics
    {
        $rg = $this->metadata->rowGroups[$rowGroup] ?? throw new \ValueError("There is no row group $rowGroup");
        $leaf = $this->metadata->schema->leaf($column) ?? throw new \ValueError("There is no column '$column'");
        $s = $rg->columns[$leaf->leafIndex ?? 0]->statistics;
        if ($s === null) {
            return null;
        }
        $minRaw = $s->minValue;
        $maxRaw = $s->maxValue;
        if ($minRaw === null && $maxRaw === null && self::oldOrderingIsReliable($leaf)) {
            $minRaw = $s->min;
            $maxRaw = $s->max;
        }
        return new ColumnStatistics(
            $this->statValue($leaf, $minRaw),
            $this->statValue($leaf, $maxRaw),
            $s->nullCount,
            $s->distinctCount,
            $s->minValue !== null ? $s->isMinValueExact : null,
            $s->maxValue !== null ? $s->isMaxValueExact : null,
        );
    }

    private static function oldOrderingIsReliable(SchemaNode $leaf): bool
    {
        switch ($leaf->physicalType) {
            case PhysicalType::Boolean:
            case PhysicalType::Float:
            case PhysicalType::Double:
                return true;
            case PhysicalType::Int32:
            case PhysicalType::Int64:
                $lt = $leaf->logicalType;
                if ($lt !== null && $lt->kind === LogicalKind::Integer && $lt->isSigned === false) {
                    return false;
                }
                return !\in_array($leaf->convertedType, [ConvertedType::Uint8, ConvertedType::Uint16, ConvertedType::Uint32, ConvertedType::Uint64], true);
            default:
                return false;
        }
    }

    private function statValue(SchemaNode $leaf, ?string $raw): bool|int|float|string|array|null
    {
        if ($raw === null) {
            return null;
        }
        $n = \strlen($raw);
        switch ($leaf->physicalType) {
            case PhysicalType::Boolean:
                $v = $n === 1 ? \ord($raw) !== 0 : null;
                break;
            case PhysicalType::Int32:
                $v = $n === 4 ? \unpack('l', $raw)[1] : null;
                break;
            case PhysicalType::Int64:
                $v = $n === 8 ? \unpack('q', $raw)[1] : null;
                break;
            case PhysicalType::Float:
                $v = $n === 4 ? \unpack('g', $raw)[1] : null;
                break;
            case PhysicalType::Double:
                $v = $n === 8 ? \unpack('e', $raw)[1] : null;
                break;
            case PhysicalType::Int96:
                $v = $n === 12 ? $raw : null;
                break;
            case PhysicalType::FixedLenByteArray:
                $v = $n === $leaf->typeLength ? $raw : null;
                break;
            default:
                $v = $raw;
        }
        if ($v === null) {
            return null;
        }
        return $this->convert([$v], $leaf)[0];
    }

    private function readFooter(): FileMetadata
    {
        $size = $this->size;
        if ($size < 12) {
            throw new ParquetException("A Parquet file is at least 12 bytes; this has $size");
        }
        $tailLen = \min($size, self::TAIL);
        $tail = $this->range($size - $tailLen, $tailLen);
        $magic = \substr($tail, -4);
        if ($magic === 'PARE') {
            throw new UnsupportedException('encrypted footer (PARE)');
        }
        if ($magic !== 'PAR1') {
            throw new ParquetException('Not a Parquet file: the last four bytes are not PAR1');
        }
        $len = \ord($tail[$tailLen - 8]) | (\ord($tail[$tailLen - 7]) << 8) | (\ord($tail[$tailLen - 6]) << 16) | (\ord($tail[$tailLen - 5]) << 24);
        if ($len < 1 || $len > $size - 12) {
            throw new ParquetException("The footer length $len does not fit in a file of $size bytes");
        }
        if ($len + 8 > $tailLen) {
            $tail = $this->range($size - 8 - $len, $len + 8 - $tailLen) . $tail;
        }
        if ($tailLen === $size && \substr($tail, 0, 4) !== 'PAR1') {
            throw new ParquetException('Not a Parquet file: it does not start with PAR1');
        }
        return MetadataParser::parse(\substr($tail, \strlen($tail) - 8 - $len, $len));
    }

    private function range(int $offset, int $length): string
    {
        ++$this->reads;
        $s = ($this->read)($offset, $length);
        if (!\is_string($s) || \strlen($s) !== $length) {
            throw new \UnexpectedValueException("The range function returned " . (\is_string($s) ? \strlen($s) : \get_debug_type($s)) . " for $length bytes at offset $offset");
        }
        return $s;
    }
}
