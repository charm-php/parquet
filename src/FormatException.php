<?php

declare(strict_types=1);

namespace Charm\Parquet;

/** The file is corrupt, truncated, or violates the Parquet format. */
final class FormatException extends ParquetException
{
}
