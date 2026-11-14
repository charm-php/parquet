<?php

declare(strict_types=1);

namespace Charm\Parquet;

enum Codec: int
{
    case Uncompressed = 0;
    case Snappy = 1;
    case Gzip = 2;
    case Lzo = 3;
    case Brotli = 4;
    case Lz4 = 5;
    case Zstd = 6;
    case Lz4Raw = 7;
}
