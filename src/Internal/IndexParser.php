<?php

declare(strict_types=1);

namespace Charm\Parquet\Internal;

use Charm\Parquet\{BoundaryOrder, ColumnIndex, ParquetException, OffsetIndex, PageLocation};
use Charm\Parquet\Thrift\CompactReader;

/** The OffsetIndex and ColumnIndex structs (Thrift compact), checked for internal consistency only. */
final class IndexParser
{
    public static function offsetIndex(string $bytes): OffsetIndex
    {
        $s = (new CompactReader($bytes))->readStruct();
        $pages = [];
        foreach (Fields::list($s, 1, 'OffsetIndex.page_locations') as $i => $p) {
            if (!\is_array($p)) {
                throw new ParquetException("Page location $i is not a struct");
            }
            $pages[] = new PageLocation(
                Fields::int($p, 1, "PageLocation $i offset"),
                Fields::int($p, 2, "PageLocation $i compressed_page_size"),
                Fields::int($p, 3, "PageLocation $i first_row_index"),
            );
        }
        return new OffsetIndex($pages);
    }

    /** @param \Closure(?string): mixed $decode raw min/max bytes to a value in the Reader's value mode */
    public static function columnIndex(string $bytes, \Closure $decode): ColumnIndex
    {
        $s = (new CompactReader($bytes))->readStruct();
        $nullPages = Fields::list($s, 1, 'ColumnIndex.null_pages');
        $mins = Fields::list($s, 2, 'ColumnIndex.min_values');
        $maxs = Fields::list($s, 3, 'ColumnIndex.max_values');
        $n = \count($nullPages);
        if (\count($mins) !== $n || \count($maxs) !== $n) {
            throw new ParquetException('ColumnIndex lists have different lengths');
        }
        $order = BoundaryOrder::tryFrom(Fields::int($s, 4, 'ColumnIndex.boundary_order')) ?? BoundaryOrder::Unordered;
        $counts = null;
        if (isset($s[5])) {
            $counts = Fields::list($s, 5, 'ColumnIndex.null_counts');
            if (\count($counts) !== $n) {
                throw new ParquetException('ColumnIndex null_counts has a different length');
            }
            foreach ($counts as $c) {
                if (!\is_int($c)) {
                    throw new ParquetException('ColumnIndex null count is not an integer');
                }
            }
        }
        $min = [];
        $max = [];
        $minRaw = [];
        $maxRaw = [];
        foreach ($nullPages as $i => $isNull) {
            if (!\is_bool($isNull) || !\is_string($mins[$i]) || !\is_string($maxs[$i])) {
                throw new ParquetException("ColumnIndex entry $i has the wrong types");
            }
            $minRaw[] = $isNull ? null : $mins[$i];
            $maxRaw[] = $isNull ? null : $maxs[$i];
            $min[] = $isNull ? null : $decode($mins[$i]);
            $max[] = $isNull ? null : $decode($maxs[$i]);
        }
        return new ColumnIndex($nullPages, $min, $max, $minRaw, $maxRaw, $order, $counts);
    }
}
