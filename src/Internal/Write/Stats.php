<?php

declare(strict_types=1);

namespace Charm\Parquet\Internal\Write;

use Charm\Parquet\PhysicalType;

/** Minimum and maximum of a page or chunk, ordered the way Parquet orders the type (unsigned bytes, signed big-endian decimals, no NaN). */
final class Stats
{
    /** Values longer than this are not used as min or max (no truncation is done). */
    private const MAX_BYTES = 4096;

    /**
     * @param list<mixed> $vals the non-null values of a page
     * @return array{mixed, mixed}|null [min, max] as PHP values, or null when there is nothing to say (no values, only NaN, too long)
     */
    public static function minMax(array $vals, string $kind): ?array
    {
        if ($vals === []) {
            return null;
        }
        switch ($kind) {
            case 'int':
                return [\min($vals), \max($vals)];
            case 'bool':
                return [\min($vals), \max($vals)];
            case 'uint64':
                $lo = $hi = $vals[0];
                foreach ($vals as $v) {
                    if (($v ^ \PHP_INT_MIN) < ($lo ^ \PHP_INT_MIN)) {
                        $lo = $v;
                    }
                    if (($v ^ \PHP_INT_MIN) > ($hi ^ \PHP_INT_MIN)) {
                        $hi = $v;
                    }
                }
                return [$lo, $hi];
            case 'float':
                if (\is_nan(\array_sum($vals))) {
                    // a NaN (or infinities that cancel): look at the values one by one and leave NaN out
                    $vals = \array_values(\array_filter($vals, static fn (float $v): bool => $v === $v));
                    if ($vals === []) {
                        return null;
                    }
                }
                $lo = \min($vals);
                $hi = \max($vals);
                return [$lo == 0 ? -0.0 : $lo, $hi == 0 ? 0.0 : $hi]; // (the spec: a zero minimum is written as -0.0 and a zero maximum as +0.0)
            case 'bytes':
                $s = $vals;
                \sort($s, \SORT_STRING);
                $lo = $s[0];
                $hi = $s[\count($s) - 1];
                return \strlen($lo) > self::MAX_BYTES || \strlen($hi) > self::MAX_BYTES ? null : [$lo, $hi];
            case 'decimal':
                $s = \array_map(static fn (string $v): string => \chr(\ord($v[0]) ^ 0x80) . \substr($v, 1), $vals);
                \sort($s, \SORT_STRING);
                $f = static fn (string $k): string => \chr(\ord($k[0]) ^ 0x80) . \substr($k, 1);
                return [$f($s[0]), $f($s[\count($s) - 1])];
        }
        return null;
    }

    /** -1, 0 or 1 */
    public static function compare(mixed $a, mixed $b, string $kind): int
    {
        switch ($kind) {
            case 'uint64':
                return ($a ^ \PHP_INT_MIN) <=> ($b ^ \PHP_INT_MIN);
            case 'bytes':
                return \strcmp($a, $b) <=> 0;
            case 'decimal':
                return \strcmp(\chr(\ord($a[0]) ^ 0x80) . \substr($a, 1), \chr(\ord($b[0]) ^ 0x80) . \substr($b, 1)) <=> 0;
            default:
                return $a <=> $b;
        }
    }

    /** The PLAIN bytes of a min or max value, as the statistics and the column index store them. */
    public static function raw(mixed $v, PhysicalType $phys): string
    {
        return match ($phys) {
            PhysicalType::Boolean => $v ? "\x01" : "\x00",
            PhysicalType::Int32 => \pack('V', $v & 0xFFFFFFFF),
            PhysicalType::Int64 => \pack('P', $v),
            PhysicalType::Float => \pack('g', $v),
            PhysicalType::Double => \pack('e', $v),
            default => $v,
        };
    }
}
