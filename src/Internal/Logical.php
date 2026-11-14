<?php

declare(strict_types=1);

namespace Charm\Parquet\Internal;

use Charm\Parquet\{ConvertedType, ParquetException, LogicalKind, PhysicalType, SchemaNode, TimeUnit, UnsupportedException};

/**
 * Logical-type conversion of a decoded column: chosen once per column chunk (converter() returns a closure for the whole list, or
 * null when the physical values are the logical values), so there is no type dispatch per value.
 *
 * Output (all exact, no dependencies):
 *   signed integers: int; UINT_8/16/32: int; UINT_64: int when it fits, else a decimal string
 *   DECIMAL: a decimal string with exactly `scale` fractional digits
 *   DATE: "YYYY-MM-DD"; TIME: "HH:MM:SS" + 3/6/9 fractional digits; TIMESTAMP: "YYYY-MM-DDTHH:MM:SS" + 3/6/9 digits + "Z" if
 *   adjusted to UTC; INT96: like TIMESTAMP with 9 digits and no suffix
 *   UUID: 36-character lower-case string; FLOAT16: float; INTERVAL: ['months' => , 'days' => , 'milliseconds' => ]
 *   everything else (strings, enums, json, bson, unknown or future annotations): the physical value
 * A logical type that does not fit the column's physical type (a DATE on INT64, a UUID on INT32) is ignored: physical values.
 * Years outside 0000-9999 are written with a '-' sign when negative and otherwise as they are (at least four digits).
 */
final class Logical
{
    private const MAX_DECIMAL_BYTES = 128;

    /** @return (\Closure(array): array)|null */
    public static function converter(SchemaNode $leaf): ?\Closure
    {
        $phys = $leaf->physicalType;
        if ($phys === PhysicalType::Int96) {
            return self::timestamp96();
        }
        $lt = $leaf->logicalType;
        $kind = $lt !== null && $lt->kind !== LogicalKind::Unrecognized && $lt->kind !== LogicalKind::Unknown ? $lt->kind : null;
        if ($kind === null) {
            return self::fromConverted($leaf, $phys);
        }
        switch ($kind) {
            case LogicalKind::Integer:
                return self::integer($phys, $lt->bitWidth ?? 0, $lt->isSigned ?? true);
            case LogicalKind::Decimal:
                return self::decimal($phys, $lt->scale ?? $leaf->scale ?? 0, $leaf->typeLength);
            case LogicalKind::Date:
                return $phys === PhysicalType::Int32 ? self::date() : null;
            case LogicalKind::Time:
                return self::time($phys, $lt->unit);
            case LogicalKind::Timestamp:
                return $phys === PhysicalType::Int64 && $lt->unit !== null ? self::timestamp($lt->unit, $lt->isAdjustedToUtc ?? false) : null;
            case LogicalKind::Uuid:
                return $phys === PhysicalType::FixedLenByteArray && $leaf->typeLength === 16 ? self::uuid() : null;
            case LogicalKind::Float16:
                return $phys === PhysicalType::FixedLenByteArray && $leaf->typeLength === 2 ? self::float16() : null;
            default:
                return null; // string, enum, json, bson, map, list, variant, geometry, geography
        }
    }

    /** @return (\Closure(array): array)|null the legacy ConvertedType annotation, used when there is no usable logical type */
    private static function fromConverted(SchemaNode $leaf, ?PhysicalType $phys): ?\Closure
    {
        switch ($leaf->convertedType) {
            case ConvertedType::Uint8:
                return self::integer($phys, 8, false);
            case ConvertedType::Uint16:
                return self::integer($phys, 16, false);
            case ConvertedType::Uint32:
                return self::integer($phys, 32, false);
            case ConvertedType::Uint64:
                return self::integer($phys, 64, false);
            case ConvertedType::Decimal:
                return self::decimal($phys, $leaf->scale ?? 0, $leaf->typeLength);
            case ConvertedType::Date:
                return $phys === PhysicalType::Int32 ? self::date() : null;
            case ConvertedType::TimeMillis:
                return self::time($phys, TimeUnit::Millis);
            case ConvertedType::TimeMicros:
                return self::time($phys, TimeUnit::Micros);
            case ConvertedType::TimestampMillis:
                return $phys === PhysicalType::Int64 ? self::timestamp(TimeUnit::Millis, true) : null;
            case ConvertedType::TimestampMicros:
                return $phys === PhysicalType::Int64 ? self::timestamp(TimeUnit::Micros, true) : null;
            case ConvertedType::Interval:
                return $phys === PhysicalType::FixedLenByteArray && $leaf->typeLength === 12 ? self::interval() : null;
            default:
                return null;
        }
    }

    /** @return (\Closure(array): array)|null */
    private static function integer(?PhysicalType $phys, int $bits, bool $signed): ?\Closure
    {
        if ($signed) {
            return null;
        }
        if ($bits === 64 && $phys === PhysicalType::Int64) {
            return static function (array $v): array {
                foreach ($v as $i => $x) {
                    if ($x !== null && $x < 0) {
                        // the unsigned value u = 2h + b as decimal text, without leaving 64 bits: u / 10 = h / 5, u % 10 = 2 (h % 5) + b
                        $h = ($x >> 1) & \PHP_INT_MAX;
                        $v[$i] = \intdiv($h, 5) . (2 * ($h % 5) + ($x & 1));
                    }
                }
                return $v;
            };
        }
        if (($bits === 8 || $bits === 16 || $bits === 32) && $phys === PhysicalType::Int32) {
            $mask = (1 << $bits) - 1;
            return static function (array $v) use ($mask): array {
                foreach ($v as $i => $x) {
                    if ($x !== null) {
                        $v[$i] = $x & $mask;
                    }
                }
                return $v;
            };
        }
        return null;
    }

    /** @return (\Closure(array): array)|null */
    private static function decimal(?PhysicalType $phys, int $scale, ?int $typeLength): ?\Closure
    {
        if ($scale < 0 || $scale > 1000) {
            throw new ParquetException("DECIMAL scale $scale is not valid");
        }
        if ($phys === PhysicalType::Int32 || $phys === PhysicalType::Int64) {
            return self::decimalInts($scale, 0);
        }
        if ($phys === PhysicalType::FixedLenByteArray && $typeLength !== null && $typeLength > 0 && $typeLength <= 8) {
            return self::decimalInts($scale, $typeLength); // (the value fits in an int: read it as one, then format as for INT64)
        }
        if ($phys === PhysicalType::FixedLenByteArray || $phys === PhysicalType::ByteArray) {
            if ($typeLength !== null && $typeLength > self::MAX_DECIMAL_BYTES) {
                throw new UnsupportedException("DECIMAL stored in $typeLength bytes (limit " . self::MAX_DECIMAL_BYTES . ')');
            }
            return static function (array $v) use ($scale): array {
                foreach ($v as $i => $x) {
                    if ($x !== null) {
                        $v[$i] = self::decimalBytes($x, $scale);
                    }
                }
                return $v;
            };
        }
        return null;
    }

    /**
     * Decimal strings from unscaled ints (INT32, INT64) or from big-endian two's complement values of $len bytes (1 to 8; 0: ints
     * already). The formatting is inline (no function call per value) and, for a small scale, the fraction strings come from a table.
     *
     * @return \Closure(array): array
     */
    private static function decimalInts(int $scale, int $len): \Closure
    {
        $p10 = $scale <= 18 ? 10 ** $scale : 0;
        $ext = $len > 0 && $len < 8 ? 1 << (8 * $len) : 0; // (what to subtract from a negative value read from $len bytes)
        return static function (array $v) use ($scale, $p10, $len, $ext): array {
            $frac = [];
            $cache = $scale > 0 && $scale <= 4;
            foreach ($v as $i => $x) {
                if ($x === null) {
                    continue;
                }
                if ($len > 0) {
                    $n = 0;
                    for ($k = 0; $k < $len; $k++) {
                        $n = ($n << 8) | \ord($x[$k]);
                    }
                    if ($ext !== 0 && $n >= ($ext >> 1)) {
                        $n -= $ext;
                    }
                    $x = $n;
                }
                if ($scale === 0) {
                    $v[$i] = (string) $x;
                    continue;
                }
                if ($x < 0) {
                    if ($x === \PHP_INT_MIN || $p10 === 0) {
                        $v[$i] = self::decimalInt($x, $scale);
                        continue;
                    }
                    $x = -$x;
                    $sign = '-';
                } elseif ($p10 === 0) {
                    $v[$i] = self::decimalInt($x, $scale);
                    continue;
                } else {
                    $sign = '';
                }
                $q = \intdiv($x, $p10);
                $r = $x - $q * $p10;
                $v[$i] = $sign . $q . '.' . ($cache ? ($frac[$r] ??= \str_pad((string) $r, $scale, '0', \STR_PAD_LEFT)) : \str_pad((string) $r, $scale, '0', \STR_PAD_LEFT));
            }
            return $v;
        };
    }

    /** The unscaled integer $x as a decimal string with $scale fractional digits. */
    public static function decimalInt(int $x, int $scale): string
    {
        $s = (string) $x;
        $neg = $s[0] === '-';
        if ($neg) {
            $s = \substr($s, 1);
        }
        return ($neg ? '-' : '') . self::pointed($s, $scale);
    }

    private static function pointed(string $digits, int $scale): string
    {
        if ($scale === 0) {
            return $digits;
        }
        $digits = \str_pad($digits, $scale + 1, '0', \STR_PAD_LEFT);
        return \substr($digits, 0, -$scale) . '.' . \substr($digits, -$scale);
    }

    /** A big-endian two's complement integer of any length ($bytes) as a decimal string with $scale fractional digits. */
    public static function decimalBytes(string $bytes, int $scale): string
    {
        $n = \strlen($bytes);
        if ($n === 0) {
            return self::pointed('0', $scale);
        }
        if ($n > self::MAX_DECIMAL_BYTES) {
            throw new UnsupportedException("DECIMAL value of $n bytes (limit " . self::MAX_DECIMAL_BYTES . ')');
        }
        $neg = (\ord($bytes[0]) & 0x80) !== 0;
        if ($n <= 8) {
            $v = 0;
            for ($i = 0; $i < $n; $i++) {
                $v = ($v << 8) | \ord($bytes[$i]);
            }
            if ($neg && $n < 8) {
                $v -= 1 << (8 * $n);
            }
            return self::decimalInt($v, $scale);
        }
        // sign-extend to a whole number of 32-bit limbs, most significant first
        $pad = (4 - $n % 4) % 4;
        $bytes = \str_repeat($neg ? "\xFF" : "\x00", $pad) . $bytes;
        /** @var list<int> $limbs */
        $limbs = \array_values(\unpack('N*', $bytes));
        if ($neg) {
            // negate: invert, then add one from the least significant limb
            $carry = 1;
            for ($i = \count($limbs) - 1; $i >= 0; $i--) {
                $x = (~$limbs[$i] & 0xFFFFFFFF) + $carry;
                $carry = $x >> 32;
                $limbs[$i] = $x & 0xFFFFFFFF;
            }
        }
        // repeated division by 10^9: each pass yields nine decimal digits
        $chunks = [];
        $start = 0;
        $count = \count($limbs);
        while ($start < $count) {
            $rem = 0;
            for ($i = $start; $i < $count; $i++) {
                $cur = ($rem << 32) | $limbs[$i];
                $limbs[$i] = \intdiv($cur, 1000000000);
                $rem = $cur % 1000000000;
            }
            $chunks[] = $rem;
            while ($start < $count && $limbs[$start] === 0) {
                $start++;
            }
        }
        $s = (string) \array_pop($chunks);
        for ($i = \count($chunks) - 1; $i >= 0; $i--) {
            $s .= \str_pad((string) $chunks[$i], 9, '0', \STR_PAD_LEFT);
        }
        return ($neg ? '-' : '') . self::pointed($s, $scale);
    }

    /** @return \Closure(array): array */
    private static function date(): \Closure
    {
        return static function (array $v): array {
            foreach ($v as $i => $x) {
                if ($x !== null) {
                    $v[$i] = self::dateString($x);
                }
            }
            return $v;
        };
    }

    /** @return (\Closure(array): array)|null */
    private static function time(?PhysicalType $phys, ?TimeUnit $unit): ?\Closure
    {
        if ($unit === null || ($phys !== PhysicalType::Int32 && $phys !== PhysicalType::Int64)) {
            return null;
        }
        [$perSec, $digits] = self::unit($unit);
        return static function (array $v) use ($perSec, $digits): array {
            foreach ($v as $i => $x) {
                if ($x !== null) {
                    $v[$i] = self::timeString($x, $perSec, $digits);
                }
            }
            return $v;
        };
    }

    /** @return \Closure(array): array */
    private static function timestamp(TimeUnit $unit, bool $utc): \Closure
    {
        [$perSec, $digits] = self::unit($unit);
        $suffix = $utc ? 'Z' : '';
        return static function (array $v) use ($perSec, $digits, $suffix): array {
            $perDay = 86400 * $perSec;
            // The date part is built once per distinct day, the "HH:MM:SS" part once per distinct second of the day, and the fraction
            // from a table of the numbers 000..999: per value that is a division or two and three array lookups, no function call.
            $dates = [];
            $secs = [];
            $d3 = self::digits3();
            foreach ($v as $i => $x) {
                if ($x === null) {
                    continue;
                }
                $days = \intdiv($x, $perDay);
                $rem = $x - $days * $perDay;
                if ($rem < 0) {
                    $rem += $perDay;
                    --$days;
                }
                $sec = \intdiv($rem, $perSec);
                $frac = $rem - $sec * $perSec;
                $head = ($dates[$days] ??= self::dateString($days) . 'T') . ($secs[$sec] ??= self::secondsString($sec));
                if ($digits === 6) {
                    $v[$i] = $head . '.' . $d3[\intdiv($frac, 1000)] . $d3[$frac % 1000] . $suffix;
                } elseif ($digits === 3) {
                    $v[$i] = $head . '.' . $d3[$frac] . $suffix;
                } else {
                    $v[$i] = $head . '.' . $d3[\intdiv($frac, 1000000)] . $d3[\intdiv($frac, 1000) % 1000] . $d3[$frac % 1000] . $suffix;
                }
            }
            return $v;
        };
    }

    /** @var array<int, string>|null */
    private static ?array $digits3 = null;

    /** "000" .. "999" by number. @return array<int, string> */
    private static function digits3(): array
    {
        return self::$digits3 ??= \array_map(static fn (int $i): string => \sprintf('%03d', $i), \range(0, 999));
    }

    /** "HH:MM:SS" for a second of the day (hours may exceed 23 for a longer span). */
    private static function secondsString(int $sec): string
    {
        return \sprintf('%02d:%02d:%02d', \intdiv($sec, 3600), \intdiv($sec % 3600, 60), $sec % 60);
    }

    /** @return \Closure(array): array */
    private static function timestamp96(): \Closure
    {
        return static function (array $v): array {
            $perDay = 86400 * 1000000000;
            foreach ($v as $i => $x) {
                if ($x !== null) {
                    if (\strlen($x) !== 12) {
                        throw new ParquetException('An INT96 value is ' . \strlen($x) . ' bytes');
                    }
                    $nanos = \unpack('P', $x)[1];
                    $julian = \unpack('l', $x, 8)[1];
                    $days = $julian - 2440588; // (the Julian day number of 1970-01-01)
                    if ($nanos < 0 || $nanos >= $perDay) {
                        // (outside one day: carry into the day count rather than print an hour of 24 or more)
                        $d = \intdiv($nanos, $perDay);
                        $nanos -= $d * $perDay;
                        if ($nanos < 0) {
                            $nanos += $perDay;
                            --$d;
                        }
                        $days += $d;
                    }
                    $v[$i] = self::dateString($days) . 'T' . self::timeString($nanos, 1000000000, 9);
                }
            }
            return $v;
        };
    }

    /** @return array{int, int} units per second and fractional digits */
    private static function unit(TimeUnit $u): array
    {
        return match ($u) {
            TimeUnit::Millis => [1000, 3],
            TimeUnit::Micros => [1000000, 6],
            TimeUnit::Nanos => [1000000000, 9],
        };
    }

    /** @return \Closure(array): array */
    private static function uuid(): \Closure
    {
        return static function (array $v): array {
            foreach ($v as $i => $x) {
                if ($x !== null) {
                    $h = \bin2hex($x);
                    $v[$i] = \substr($h, 0, 8) . '-' . \substr($h, 8, 4) . '-' . \substr($h, 12, 4) . '-' . \substr($h, 16, 4) . '-' . \substr($h, 20, 12);
                }
            }
            return $v;
        };
    }

    /** @return \Closure(array): array */
    private static function float16(): \Closure
    {
        return static function (array $v): array {
            foreach ($v as $i => $x) {
                if ($x !== null) {
                    $h = \ord($x[0]) | (\ord($x[1]) << 8);
                    $sign = $h & 0x8000 ? -1.0 : 1.0;
                    $exp = ($h >> 10) & 0x1F;
                    $man = $h & 0x3FF;
                    if ($exp === 0) {
                        $f = $sign * $man * 5.9604644775390625e-8; // 2^-24
                    } elseif ($exp === 31) {
                        $f = $man === 0 ? $sign * \INF : \NAN;
                    } else {
                        $f = $sign * (1 + $man / 1024) * (2.0 ** ($exp - 15));
                    }
                    $v[$i] = $f;
                }
            }
            return $v;
        };
    }

    /** @return \Closure(array): array */
    private static function interval(): \Closure
    {
        return static function (array $v): array {
            foreach ($v as $i => $x) {
                if ($x !== null) {
                    $u = \unpack('V3', $x);
                    $v[$i] = ['months' => $u[1], 'days' => $u[2], 'milliseconds' => $u[3]];
                }
            }
            return $v;
        };
    }

    /** "YYYY-MM-DD" for a day count since 1970-01-01 (proleptic Gregorian; integer arithmetic only). */
    public static function dateString(int $days): string
    {
        $z = $days + 719468;
        $era = \intdiv($z >= 0 ? $z : $z - 146096, 146097);
        $doe = $z - $era * 146097;
        $yoe = \intdiv($doe - \intdiv($doe, 1460) + \intdiv($doe, 36524) - \intdiv($doe, 146096), 365);
        $doy = $doe - (365 * $yoe + \intdiv($yoe, 4) - \intdiv($yoe, 100));
        $mp = \intdiv(5 * $doy + 2, 153);
        $d = $doy - \intdiv(153 * $mp + 2, 5) + 1;
        $m = $mp < 10 ? $mp + 3 : $mp - 9;
        $y = $yoe + $era * 400 + ($m <= 2 ? 1 : 0);
        return ($y < 0 ? \sprintf('-%04d', -$y) : \sprintf('%04d', $y)) . \sprintf('-%02d-%02d', $m, $d);
    }

    /** "HH:MM:SS" and $digits fractional digits for $x units, $perSec per second; a negative time gets a '-'. Hours may exceed 23. */
    public static function timeString(int $x, int $perSec, int $digits): string
    {
        $neg = $x < 0;
        if ($neg) {
            $x = $x === \PHP_INT_MIN ? \PHP_INT_MAX : -$x;
        }
        $sec = \intdiv($x, $perSec);
        $frac = $x - $sec * $perSec;
        return ($neg ? '-' : '') . \sprintf('%02d:%02d:%02d.%0' . $digits . 'd', \intdiv($sec, 3600), \intdiv($sec % 3600, 60), $sec % 60, $frac);
    }
}
