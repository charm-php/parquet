<?php

declare(strict_types=1);

namespace Charm\Parquet\Tests;

use Charm\Parquet\PhysicalType;

/**
 * The canonical text form of physical values that the expected-value JSON files (made by pyarrow, see tests/tools/oracle.py) use:
 * BOOLEAN "t"/"f"; INT32/INT64 decimal digits; FLOAT 8 hex digits and DOUBLE 16 hex digits (the big-endian IEEE bit pattern, so
 * NaN and -0.0 compare exactly); byte strings in hex; null stays null.
 */
final class Canonical
{
    public static function value(PhysicalType $type, bool|int|float|string|null $v): ?string
    {
        if ($v === null) {
            return null;
        }
        return match ($type) {
            PhysicalType::Boolean => $v ? 't' : 'f',
            PhysicalType::Int32, PhysicalType::Int64 => (string) $v,
            PhysicalType::Float => \bin2hex(\pack('G', $v)),
            PhysicalType::Double => \bin2hex(\pack('E', $v)),
            default => \bin2hex((string) $v),
        };
    }

    /**
     * A logical value, by its PHP type: bool t/f; int decimal digits; float the IEEE double bits in big-endian hex ("nan" for NaN);
     * string hex of its bytes; array (INTERVAL) hex of "months,days,milliseconds".
     */
    public static function logical(bool|int|float|string|array|null $v): ?string
    {
        return match (true) {
            $v === null => null,
            \is_bool($v) => $v ? 't' : 'f',
            \is_int($v) => (string) $v,
            \is_float($v) => \is_nan($v) ? 'nan' : \bin2hex(\pack('E', $v)),
            \is_array($v) => \bin2hex(\implode(',', $v)),
            default => \bin2hex($v),
        };
    }

    /**
     * A possibly nested value as a canonical tree (see tests/tools/oracle.py): null; a string for a scalar (by its PHP type, as
     * logical()); a list for a PHP list; {"o": [[key, value], ...]} for an associative array, the key being "i<int>" or "s<hex>".
     */
    public static function tree(mixed $v): mixed
    {
        if ($v === null || !\is_array($v)) {
            return self::logical($v);
        }
        if (\array_is_list($v)) {
            return \array_map(self::tree(...), $v);
        }
        $pairs = [];
        foreach ($v as $k => $x) {
            $pairs[] = [\is_int($k) ? 'i' . $k : 's' . \bin2hex($k), self::tree($x)];
        }
        return ['o' => $pairs];
    }

    /**
     * @param list<bool|int|float|string|null> $values
     * @return list<?string>
     */
    public static function column(PhysicalType $type, array $values): array
    {
        $out = [];
        foreach ($values as $v) {
            $out[] = self::value($type, $v);
        }
        return $out;
    }
}
