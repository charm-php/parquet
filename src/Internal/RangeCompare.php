<?php

declare(strict_types=1);

namespace Charm\Parquet\Internal;

use Charm\Parquet\{ConvertedType, LogicalKind, PhysicalType, SchemaNode, ValueMode};

/**
 * Comparison of two values of one column for pruning. Returns -1, 0 or 1, or null when the comparison is not safe (different kinds of
 * value, NaN, a calendar string whose year has a sign or more than four digits, an array): the caller then keeps the data.
 */
final class RangeCompare
{
    public static function compare(mixed $a, mixed $b, SchemaNode $leaf, ValueMode $mode): ?int
    {
        if ($a === null || $b === null) {
            return null;
        }
        if (\is_float($a) || \is_float($b)) {
            if (!(\is_float($a) || \is_int($a)) || !(\is_float($b) || \is_int($b)) || (\is_float($a) && \is_nan($a)) || (\is_float($b) && \is_nan($b))) {
                return null;
            }
            return $a <=> $b;
        }
        if (\is_bool($a) || \is_bool($b)) {
            return \is_bool($a) && \is_bool($b) ? (int) $a <=> (int) $b : null;
        }
        if (\is_int($a) && \is_int($b)) {
            return $a <=> $b;
        }
        if (!\is_string($a) && !\is_int($a) || !\is_string($b) && !\is_int($b)) {
            return null;
        }
        $kind = self::kind($leaf);
        if (\is_int($a) !== \is_int($b) && $kind !== 'uint64' && $kind !== 'decimal') {
            return null; // (an int against a string: not the same kind of value)
        }
        $a = (string) $a;
        $b = (string) $b;
        if ($kind === 'unordered' || ($mode === ValueMode::Physical && $kind === 'float16')) {
            return null; // (INT96, INTERVAL: the format defines no order; FLOAT16 bytes: not ordered as bytes)
        }
        if ($mode === ValueMode::Physical && $kind === 'decimal') {
            return self::signedBytes($a, $b);
        }
        // strings from here on (an int mixed with a string is compared as decimal text: UINT_64 values are either)
        if ($mode === ValueMode::Logical || \is_numeric($a) && \is_numeric($b)) {
            if ($kind === 'decimal') {
                return self::decimal($a, $b);
            }
            if ($kind === 'calendar') {
                if (!\preg_match('/^\d{4}-/', $a) || !\preg_match('/^\d{4}-/', $b)) {
                    return null;
                }
                return \strcmp($a, $b) <=> 0;
            }
            if ($kind === 'uint64') {
                return \ctype_digit($a) && \ctype_digit($b) ? self::decimal($a, $b) : null;
            }
        }
        return \strcmp($a, $b) <=> 0;
    }

    private static function kind(SchemaNode $leaf): string
    {
        $k = $leaf->logicalType?->kind;
        $c = $leaf->convertedType;
        if ($leaf->physicalType === PhysicalType::Int96 || $c === ConvertedType::Interval) {
            return 'unordered';
        }
        if ($k === LogicalKind::Float16) {
            return 'float16';
        }
        if ($k === LogicalKind::Decimal || $c === ConvertedType::Decimal) {
            return 'decimal';
        }
        if (\in_array($k, [LogicalKind::Date, LogicalKind::Time, LogicalKind::Timestamp], true)
            || \in_array($c, [ConvertedType::Date, ConvertedType::TimeMillis, ConvertedType::TimeMicros, ConvertedType::TimestampMillis, ConvertedType::TimestampMicros], true)) {
            return 'calendar';
        }
        if (($k === LogicalKind::Integer && $leaf->logicalType?->isSigned === false && $leaf->logicalType->bitWidth === 64) || $c === ConvertedType::Uint64) {
            return 'uint64';
        }
        return 'bytes';
    }

    /** Two big-endian two's complement integers of any lengths, compared by value. */
    private static function signedBytes(string $a, string $b): int
    {
        $na = $a !== '' && (\ord($a[0]) & 0x80) !== 0;
        $nb = $b !== '' && (\ord($b[0]) & 0x80) !== 0;
        if ($na !== $nb) {
            return $na ? -1 : 1;
        }
        $len = \max(\strlen($a), \strlen($b));
        $pad = $na ? "\xFF" : "\x00";
        return \strcmp(\str_pad($a, $len, $pad, \STR_PAD_LEFT), \str_pad($b, $len, $pad, \STR_PAD_LEFT)) <=> 0;
    }

    /** Two decimal strings ("-12.50", "7", "0.05"), compared by value; null when either is not a plain decimal number. */
    private static function decimal(string $a, string $b): ?int
    {
        if (!\preg_match('/^(-?)(\d+)(?:\.(\d*))?$/', $a, $x) || !\preg_match('/^(-?)(\d+)(?:\.(\d*))?$/', $b, $y)) {
            return null;
        }
        $na = $x[1] === '-';
        $nb = $y[1] === '-';
        $ia = \ltrim($x[2], '0');
        $ib = \ltrim($y[2], '0');
        $fa = \rtrim($x[3] ?? '', '0');
        $fb = \rtrim($y[3] ?? '', '0');
        $za = $ia === '' && $fa === '';
        $zb = $ib === '' && $fb === '';
        if ($za) {
            $na = false;
        }
        if ($zb) {
            $nb = false;
        }
        if ($na !== $nb) {
            return $na ? -1 : 1;
        }
        $len = \strlen($ia) <=> \strlen($ib);
        $mag = $len !== 0 ? $len : (\strcmp($ia, $ib) !== 0 ? \strcmp($ia, $ib) <=> 0 : \strcmp(\str_pad($fa, \max(\strlen($fa), \strlen($fb)), '0'), \str_pad($fb, \max(\strlen($fa), \strlen($fb)), '0')) <=> 0);
        return $na ? -$mag : $mag;
    }
}
