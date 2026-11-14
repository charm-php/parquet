<?php

declare(strict_types=1);

namespace Charm\Parquet\Internal\Write;

use Charm\Parquet\Codec;

final class WriteOptions
{
    public function __construct(
        public readonly Codec $codec,
        public readonly int $dataPageSize,
        public readonly bool $dictionary,
        public readonly int $dictionaryPageSize,
        public readonly int $pageVersion,
        public readonly bool $statistics,
        public readonly bool $pageIndex,
    ) {
    }

    public function compress(string $body): string
    {
        return match ($this->codec) {
            Codec::Uncompressed => $body,
            Codec::Snappy => SnappyEncoder::compress($body),
            Codec::Gzip => (string) \gzencode($body, 6),
            Codec::Zstd => \Charm\Zstd\Zstd::compress($body),
            default => throw new \Charm\Parquet\UnsupportedException('writing with codec ' . $this->codec->name),
        };
    }
}
