<?php

declare(strict_types=1);

namespace Charm\Parquet;

final readonly class ColumnChunk
{
    public function __construct(
        /** The leaf this chunk stores */
        public SchemaNode $leaf,
        public PhysicalType $physicalType,
        /** @var list<Encoding> the encodings this version knows; ids it does not know are only in $encodingIds */
        public array $encodings,
        /** The codec, or null when its id is not one this version knows (see $codecId): reading the column then throws UnsupportedException */
        public ?Codec $codec,
        public int $numValues,
        public int $totalUncompressedSize,
        public int $totalCompressedSize,
        public int $dataPageOffset,
        public ?int $indexPageOffset,
        public ?int $dictionaryPageOffset,
        public ?Statistics $statistics,
        /** @var array<string, ?string> */
        public array $keyValueMetadata,
        /** ColumnChunk.file_path: data in another file (not supported for reading) */
        public ?string $filePath,
        public int $fileOffset,
        public ?int $offsetIndexOffset,
        public ?int $offsetIndexLength,
        public ?int $columnIndexOffset,
        public ?int $columnIndexLength,
        public ?int $bloomFilterOffset,
        public ?int $bloomFilterLength,
        /** The column is encrypted (ColumnChunk.crypto_metadata is present): its pages cannot be read */
        public bool $encrypted = false,
        /** The raw codec id from the footer */
        public int $codecId = 0,
        /** @var list<int> every encoding id from the footer, known or not */
        public array $encodingIds = [],
        /** Why this chunk cannot be read (for example missing or encrypted column metadata), or null. Only reading this column fails. */
        public ?string $unsupported = null,
    ) {
    }

    /** Where the chunk's first page (the dictionary page, if there is one) starts. */
    public function startOffset(): int
    {
        $d = $this->dictionaryPageOffset;
        return $d !== null && $d > 0 && $d < $this->dataPageOffset ? $d : $this->dataPageOffset;
    }
}
