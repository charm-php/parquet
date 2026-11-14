<?php

declare(strict_types=1);

namespace Charm\Parquet;

/**
 * The per-page statistics of one column chunk, one entry per data page in the order of the OffsetIndex. For a page that holds only
 * nulls ($nullPages[i]) there is no min or max (null). min and max are decoded in the Reader's value mode (null where the bytes cannot
 * be decoded); the raw bytes stay available in $minRaw and $maxRaw.
 */
final readonly class ColumnIndex
{
    public function __construct(
        /** @var list<bool> */
        public array $nullPages,
        /** @var list<bool|int|float|string|array|null> */
        public array $min,
        /** @var list<bool|int|float|string|array|null> */
        public array $max,
        /** @var list<?string> */
        public array $minRaw,
        /** @var list<?string> */
        public array $maxRaw,
        public BoundaryOrder $boundaryOrder,
        /** @var list<int>|null */
        public ?array $nullCounts,
    ) {
    }
}
