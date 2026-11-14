<?php

declare(strict_types=1);

namespace Charm\Parquet;

enum Repetition: int
{
    case Required = 0;
    case Optional = 1;
    case Repeated = 2;
}
