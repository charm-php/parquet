<?php

declare(strict_types=1);

namespace Charm\Parquet\Internal\Write;

use Charm\Parquet\{ConvertedType, PhysicalType, TimeUnit, Type};
use Charm\Parquet\Thrift\CompactWriter as T;

/**
 * How a leaf Type is stored: its physical type, the annotations for the footer, how a PHP value becomes the physical value (conv: a
 * TypeError for a value of the wrong kind, a ValueError for one that does not fit), and how its statistics are ordered.
 */
final class LeafSpec
{
    /**
     * @param \Closure(mixed): mixed $conv
     * @param list<array{int, int, mixed}>|null $logical the fields of the LogicalType union (one field), or null
     */
    private function __construct(
        public readonly PhysicalType $phys,
        public readonly ?int $length,
        public readonly \Closure $conv,
        /** 'int' (ordered as PHP ints), 'uint64', 'float', 'bool', 'bytes' (unsigned bytes), 'decimal' (signed big-endian bytes) or 'none' (no statistics) */
        public readonly string $stat,
        public readonly ?int $converted = null,
        public readonly ?array $logical = null,
        public readonly ?int $scale = null,
        public readonly ?int $precision = null,
        /** the estimated size of a PLAIN value in bytes (0: variable, then 4 + length) */
        public readonly int $width = 0,
        public readonly bool $dictionary = false,
    ) {
    }

    public static function forType(Type $t): self
    {
        $p = $t->params;
        switch ($t->kind) {
            case 'bool':
                return new self(PhysicalType::Boolean, null, static fn (mixed $v): bool => \is_bool($v) ? $v : throw self::type('bool', $v), 'bool', width: 1);
            case 'int8':
                return self::int(PhysicalType::Int32, 8, true, ConvertedType::Int8);
            case 'int16':
                return self::int(PhysicalType::Int32, 16, true, ConvertedType::Int16);
            case 'int32':
                return self::int(PhysicalType::Int32, 32, true, null);
            case 'int64':
                return self::int(PhysicalType::Int64, 64, true, null);
            case 'uint8':
                return self::int(PhysicalType::Int32, 8, false, ConvertedType::Uint8);
            case 'uint16':
                return self::int(PhysicalType::Int32, 16, false, ConvertedType::Uint16);
            case 'uint32':
                return self::int(PhysicalType::Int32, 32, false, ConvertedType::Uint32);
            case 'uint64':
                return self::uint64();
            case 'float':
                return new self(PhysicalType::Float, null, static fn (mixed $v): float => \is_float($v) ? $v : (\is_int($v) ? (float) $v : throw self::type('float', $v)), 'float', width: 4, dictionary: false);
            case 'double':
                return new self(PhysicalType::Double, null, static fn (mixed $v): float => \is_float($v) ? $v : (\is_int($v) ? (float) $v : throw self::type('float', $v)), 'float', width: 8);
            case 'float16':
                return new self(PhysicalType::FixedLenByteArray, 2, static fn (mixed $v): string => \pack('v', self::half(\is_float($v) ? $v : (\is_int($v) ? (float) $v : throw self::type('float', $v)))),
                    'none', logical: [[15, T::STRUCT, []]], width: 2, dictionary: true);
            case 'string':
                return new self(PhysicalType::ByteArray, null, static function (mixed $v): string {
                    if (!\is_string($v)) {
                        throw self::type('string', $v);
                    }
                    return \preg_match('//u', $v) === 1 ? $v : throw new \ValueError('the string is not valid UTF-8');
                }, 'bytes', ConvertedType::Utf8->value, [[1, T::STRUCT, []]], dictionary: true);
            case 'json':
                return new self(PhysicalType::ByteArray, null, static function (mixed $v): string {
                    if (!\is_string($v)) {
                        throw self::type('string', $v);
                    }
                    return \preg_match('//u', $v) === 1 ? $v : throw new \ValueError('the JSON text is not valid UTF-8');
                }, 'bytes', ConvertedType::Json->value, [[12, T::STRUCT, []]], dictionary: true);
            case 'bytes':
                return new self(PhysicalType::ByteArray, null, static fn (mixed $v): string => \is_string($v) ? $v : throw self::type('string', $v), 'bytes', dictionary: true);
            case 'fixed':
                $n = $p['length'];
                return new self(PhysicalType::FixedLenByteArray, $n, static function (mixed $v) use ($n): string {
                    if (!\is_string($v)) {
                        throw self::type('string', $v);
                    }
                    return \strlen($v) === $n ? $v : throw new \ValueError('expected ' . $n . ' bytes, got ' . \strlen($v));
                }, 'bytes', width: $n, dictionary: true);
            case 'uuid':
                return new self(PhysicalType::FixedLenByteArray, 16, static function (mixed $v): string {
                    if (!\is_string($v)) {
                        throw self::type('string', $v);
                    }
                    if (\preg_match('/^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/', $v) !== 1) {
                        throw new \ValueError('not a UUID in the 8-4-4-4-12 form');
                    }
                    return (string) \hex2bin(\str_replace('-', '', $v));
                }, 'bytes', logical: [[14, T::STRUCT, []]], width: 16, dictionary: true);
            case 'decimal':
                return self::decimal($p['precision'], $p['scale']);
            case 'date':
                return new self(PhysicalType::Int32, null, static fn (mixed $v): int => Temporal::date($v), 'int', ConvertedType::Date->value, [[6, T::STRUCT, []]], width: 4, dictionary: true);
            case 'time':
                return self::time($p['unit'], $p['utc']);
            case 'timestamp':
                return self::timestamp($p['unit'], $p['utc']);
        }
        throw new \LogicException('not a leaf type: ' . $t->kind);
    }

    private static function type(string $expected, mixed $got): \TypeError
    {
        return new \TypeError("expected $expected, got " . \get_debug_type($got));
    }

    private static function int(PhysicalType $phys, int $bits, bool $signed, ?ConvertedType $conv): self
    {
        $min = $signed ? ($bits === 64 ? \PHP_INT_MIN : -(1 << ($bits - 1))) : 0;
        $max = $signed ? ($bits === 64 ? \PHP_INT_MAX : (1 << ($bits - 1)) - 1) : (1 << $bits) - 1;
        $logical = $bits === 32 && $signed && $phys === PhysicalType::Int32 || $bits === 64 && $signed ? null : [[10, T::STRUCT, [[1, T::BYTE, $bits], [2, T::BOOL, $signed]]]];
        return new self($phys, null, static function (mixed $v) use ($min, $max): int {
            if (!\is_int($v)) {
                throw self::type('int', $v);
            }
            return $v >= $min && $v <= $max ? $v : throw new \ValueError("$v is outside $min .. $max");
        }, 'int', $conv?->value, $logical, width: $phys === PhysicalType::Int64 ? 8 : 4, dictionary: true);
    }

    private static function uint64(): self
    {
        return new self(PhysicalType::Int64, null, static function (mixed $v): int {
            if (\is_int($v)) {
                return $v >= 0 ? $v : throw new \ValueError("$v is negative");
            }
            if (!\is_string($v) || \preg_match('/^\d+$/', $v) !== 1) {
                throw self::type('int or a string of digits', $v);
            }
            $v = \ltrim($v, '0');
            if ($v === '') {
                return 0;
            }
            if (\strlen($v) > 20 || (\strlen($v) === 20 && \strcmp($v, '18446744073709551615') > 0)) {
                throw new \ValueError("$v does not fit in 64 bits");
            }
            return \unpack('J', Temporal::decimalToBytes($v, false, 8, false))[1];
        }, 'uint64', ConvertedType::Uint64->value, [[10, T::STRUCT, [[1, T::BYTE, 64], [2, T::BOOL, false]]]], width: 8, dictionary: true);
    }

    private static function decimal(int $precision, int $scale): self
    {
        $phys = $precision <= 9 ? PhysicalType::Int32 : ($precision <= 18 ? PhysicalType::Int64 : PhysicalType::FixedLenByteArray);
        $len = $phys === PhysicalType::FixedLenByteArray ? (int) \ceil(($precision * \log(10, 2) + 1 + 1e-9) / 8) : null;
        $logical = [[5, T::STRUCT, [[1, T::I32, $scale], [2, T::I32, $precision]]]];
        $conv = static function (mixed $v) use ($precision, $scale, $phys, $len): int|string {
            if (\is_int($v)) {
                $v = (string) $v;
            } elseif (!\is_string($v)) {
                throw self::type('a decimal string or int', $v);
            }
            if (\preg_match('/^([+-]?)(\d+)(?:\.(\d*))?$/', $v, $m) !== 1) {
                throw new \ValueError("'$v' is not a decimal number");
            }
            $frac = $m[3] ?? '';
            if (\strlen($frac) > $scale) {
                throw new \ValueError("'$v' has more than $scale fractional digits");
            }
            $digits = \ltrim($m[2] . \str_pad($frac, $scale, '0'), '0');
            if (\strlen($digits) > $precision) {
                throw new \ValueError("'$v' has more than $precision digits");
            }
            $neg = $m[1] === '-' && $digits !== '';
            if ($phys !== PhysicalType::FixedLenByteArray) {
                return $digits === '' ? 0 : ($neg ? -(int) $digits : (int) $digits);
            }
            return Temporal::decimalToBytes($digits === '' ? '0' : $digits, $neg, $len);
        };
        return new self($phys, $len, $conv, $phys === PhysicalType::FixedLenByteArray ? 'decimal' : 'int', ConvertedType::Decimal->value, $logical, $scale, $precision,
            width: $phys === PhysicalType::Int32 ? 4 : ($phys === PhysicalType::Int64 ? 8 : $len), dictionary: true);
    }

    private static function unitStruct(TimeUnit $u): array
    {
        return [[$u->value, T::STRUCT, []]];
    }

    private static function time(TimeUnit $unit, bool $utc): self
    {
        $phys = $unit === TimeUnit::Millis ? PhysicalType::Int32 : PhysicalType::Int64;
        $conv = match ($unit) {
            TimeUnit::Millis => ConvertedType::TimeMillis->value,
            TimeUnit::Micros => ConvertedType::TimeMicros->value,
            TimeUnit::Nanos => null,
        };
        $min = $phys === PhysicalType::Int32 ? -(1 << 31) : \PHP_INT_MIN;
        $max = $phys === PhysicalType::Int32 ? (1 << 31) - 1 : \PHP_INT_MAX;
        return new self($phys, null, static fn (mixed $v): int => Temporal::time($v, $unit, $min, $max), 'int', $conv,
            [[7, T::STRUCT, [[1, T::BOOL, $utc], [2, T::STRUCT, self::unitStruct($unit)]]]], width: $phys === PhysicalType::Int32 ? 4 : 8, dictionary: true);
    }

    private static function timestamp(TimeUnit $unit, bool $utc): self
    {
        $conv = $utc ? match ($unit) {
            TimeUnit::Millis => ConvertedType::TimestampMillis->value,
            TimeUnit::Micros => ConvertedType::TimestampMicros->value,
            TimeUnit::Nanos => null,
        } : null;
        return new self(PhysicalType::Int64, null, static fn (mixed $v): int => Temporal::timestamp($v, $unit, $utc), 'int', $conv,
            [[8, T::STRUCT, [[1, T::BOOL, $utc], [2, T::STRUCT, self::unitStruct($unit)]]]], width: 8, dictionary: true);
    }

    /** A double as the 16 bits of an IEEE half precision float, rounded to nearest even; a finite value above the half range is a ValueError. */
    private static function half(float $f): int
    {
        if (\is_nan($f)) {
            return 0x7E00;
        }
        $b = \unpack('V', \pack('g', $f))[1];
        $sign = ($b >> 16) & 0x8000;
        $exp = ($b >> 23) & 0xFF;
        $man = $b & 0x7FFFFF;
        if ($exp === 0xFF) {
            return $sign | 0x7C00;
        }
        $e = $exp - 127 + 15;
        if ($e >= 31) {
            throw new \ValueError("$f is outside the range of a half precision float");
        }
        if ($e <= 0) {
            if ($e < -10) {
                return $sign;
            }
            $man |= 0x800000;
            $shift = 14 - $e;
            $half = $man >> $shift;
            $rem = $man & ((1 << $shift) - 1);
            $mid = 1 << ($shift - 1);
            if ($rem > $mid || ($rem === $mid && ($half & 1) === 1)) {
                ++$half;
            }
            return $sign | $half;
        }
        $half = ($e << 10) | ($man >> 13);
        $rem = $man & 0x1FFF;
        if ($rem > 0x1000 || ($rem === 0x1000 && ($half & 1) === 1)) {
            ++$half;
        }
        if (($half & 0x7FFF) >= 0x7C00) {
            throw new \ValueError("$f is outside the range of a half precision float");
        }
        return $sign | $half;
    }
}
