<?php

declare(strict_types=1);

namespace Charm\Parquet;

/** What the reader returns for a column: the stored (physical) values, or the values its logical type describes (the default). */
enum ValueMode
{
    case Physical;
    case Logical;
}
