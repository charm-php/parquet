<?php

declare(strict_types=1);

namespace Charm\Parquet;

/** The file is valid but uses something this version does not implement; the message names the feature (for example "encoding DELTA_BINARY_PACKED"). */
final class UnsupportedException extends ParquetException
{
}
