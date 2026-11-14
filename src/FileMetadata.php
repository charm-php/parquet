<?php

declare(strict_types=1);

namespace Charm\Parquet;

final readonly class FileMetadata
{
    public function __construct(
        public int $version,
        public Schema $schema,
        public int $numRows,
        /** @var list<RowGroup> */
        public array $rowGroups,
        /** @var array<string, ?string> */
        public array $keyValueMetadata,
        public ?string $createdBy,
    ) {
    }

    /** The rows in all row groups added up (numRows is the footer's own claim, which some writers get wrong). */
    public function rowGroupRowCount(): int
    {
        $n = 0;
        foreach ($this->rowGroups as $g) {
            $n += $g->numRows;
        }
        return $n;
    }
}
