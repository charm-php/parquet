<?php

declare(strict_types=1);

namespace Charm\Parquet;

/** How a Parquet MAP is returned. */
enum MapMode
{
    /**
     * Decided by the schema type of the key, never by the values (so a column always has one shape): an associative array
     * key => value when the key is stored as INT32, INT64, BYTE_ARRAY or FIXED_LEN_BYTE_ARRAY and its value in the current
     * ValueMode is an int or a string; otherwise a list of [key, value] pairs. PHP turns numeric-string keys into int keys, and
     * a key that occurs twice keeps its last value.
     */
    case Auto;
    /** A list of [key, value] pairs for every map: nothing is merged or converted. */
    case Pairs;
}
