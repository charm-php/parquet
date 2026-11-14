<?php

declare(strict_types=1);

namespace Charm\Parquet;

/** The base of everything this package throws for a file it cannot read (and for a failing range function). */
class ParquetException extends \RuntimeException
{
}
