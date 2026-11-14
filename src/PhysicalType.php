<?php

declare(strict_types=1);

namespace Charm\Parquet;

enum PhysicalType: int
{
    case Boolean = 0;
    case Int32 = 1;
    case Int64 = 2;
    case Int96 = 3;
    case Float = 4;
    case Double = 5;
    case ByteArray = 6;
    case FixedLenByteArray = 7;
}
