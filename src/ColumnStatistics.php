<?php

declare(strict_types=1);

namespace Charm\Parquet;

/**
 * The statistics of one column chunk with min and max decoded to PHP values, in the Reader's value mode (so a DATE column's min
 * is a "YYYY-MM-DD" string in logical mode and an int in physical mode). The raw bytes stay available in ColumnChunk::$statistics.
 *
 * min and max come from the footer's min_value/max_value; the deprecated min/max are used only when those are absent and only for
 * types whose old ordering is not in doubt (booleans, signed integers, floats): old writers ordered unsigned integers and byte
 * strings by their signed bytes, which says nothing reliable. For those, and when the bytes have the wrong size, they are null.
 */
final readonly class ColumnStatistics
{
    public function __construct(
        public bool|int|float|string|array|null $min,
        public bool|int|float|string|array|null $max,
        public ?int $nullCount,
        public ?int $distinctCount,
        /** true/false as the footer says (is_min_value_exact), null when it does not say (always null for the deprecated fields) */
        public ?bool $isMinExact,
        public ?bool $isMaxExact,
    ) {
    }
}
