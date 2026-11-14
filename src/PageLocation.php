<?php

declare(strict_types=1);

namespace Charm\Parquet;

/** Where one data page of a column chunk is: its offset in the file, its size with its header, and the row it starts at (in the row group). */
final readonly class PageLocation
{
    public function __construct(
        public int $offset,
        public int $compressedSize,
        public int $firstRowIndex,
    ) {
    }
}
