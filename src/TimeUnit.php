<?php

declare(strict_types=1);

namespace Charm\Parquet;

enum TimeUnit: int
{
    case Millis = 1;
    case Micros = 2;
    case Nanos = 3;
}
