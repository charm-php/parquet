<?php

declare(strict_types=1);

namespace Charm\Parquet;

enum Encoding: int
{
    case Plain = 0;
    case PlainDictionary = 2;
    case Rle = 3;
    case BitPacked = 4;
    case DeltaBinaryPacked = 5;
    case DeltaLengthByteArray = 6;
    case DeltaByteArray = 7;
    case RleDictionary = 8;
    case ByteStreamSplit = 9;
    case Alp = 10;
}
