<?php

declare(strict_types=1);

namespace Charm\Parquet;

/**
 * The file is corrupt, truncated, or violates the Parquet format. It is an \UnexpectedValueException, so
 * `catch (\UnexpectedValueException)` catches a data error from any charm package.
 *
 * `UnsupportedException` extends it: a valid file that uses something this version does not implement.
 */
class ParquetException extends \UnexpectedValueException
{
}
