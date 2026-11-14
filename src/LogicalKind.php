<?php

declare(strict_types=1);

namespace Charm\Parquet;

enum LogicalKind: int
{
    case String = 1;
    case Map = 2;
    case List = 3;
    case Enum = 4;
    case Decimal = 5;
    case Date = 6;
    case Time = 7;
    case Timestamp = 8;
    case Integer = 10;
    case Unknown = 11;
    case Json = 12;
    case Bson = 13;
    case Uuid = 14;
    case Float16 = 15;
    case Variant = 16;
    case Geometry = 17;
    case Geography = 18;
    case Unrecognized = 0;
}
