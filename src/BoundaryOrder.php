<?php

declare(strict_types=1);

namespace Charm\Parquet;

/** The order of the page minimums and maximums of a column index. */
enum BoundaryOrder: int
{
    case Unordered = 0;
    case Ascending = 1;
    case Descending = 2;
}
