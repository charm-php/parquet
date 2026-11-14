<?php

declare(strict_types=1);

namespace Charm\Parquet;

final readonly class RowGroup
{
    public function __construct(
        /** @var list<ColumnChunk> one per leaf, in schema order */
        public array $columns,
        public int $totalByteSize,
        public int $numRows,
        public ?int $fileOffset,
        public ?int $totalCompressedSize,
        public ?int $ordinal,
    ) {
    }
}
