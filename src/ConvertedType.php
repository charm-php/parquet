<?php

declare(strict_types=1);

namespace Charm\Parquet;

enum ConvertedType: int
{
    case Utf8 = 0;
    case Map = 1;
    case MapKeyValue = 2;
    case List = 3;
    case Enum = 4;
    case Decimal = 5;
    case Date = 6;
    case TimeMillis = 7;
    case TimeMicros = 8;
    case TimestampMillis = 9;
    case TimestampMicros = 10;
    case Uint8 = 11;
    case Uint16 = 12;
    case Uint32 = 13;
    case Uint64 = 14;
    case Int8 = 15;
    case Int16 = 16;
    case Int32 = 17;
    case Int64 = 18;
    case Json = 19;
    case Bson = 20;
    case Interval = 21;
}
