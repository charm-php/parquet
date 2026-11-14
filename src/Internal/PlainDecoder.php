<?php

declare(strict_types=1);

namespace Charm\Parquet\Internal;

use Charm\Parquet\{ParquetException, PhysicalType};

/** PLAIN-encoded values: one unpack() or str_split() per page for fixed-width types, two native calls per BYTE_ARRAY value. */
final class PlainDecoder
{
    /**
     * $n values of $type from $raw starting at $p.
     *
     * @return list<bool|int|float|string>
     */
    public static function decode(string $raw, int $p, PhysicalType $type, int $n, ?int $typeLength): array
    {
        if ($n <= 0) {
            return [];
        }
        $rawLen = \strlen($raw);
        $avail = $rawLen - $p;
        switch ($type) {
            case PhysicalType::Int32:
                self::need($n * 4, $avail, $type);
                /** @var array<int, int> $v */
                $v = \unpack(self::littleEndian() ? 'l*' : 'V*', \substr($raw, $p, $n * 4));
                if (!self::littleEndian()) {
                    foreach ($v as $k => $x) {
                        $v[$k] = $x >= 0x80000000 ? $x - 0x100000000 : $x;
                    }
                }
                return \array_values($v);
            case PhysicalType::Int64:
                self::need($n * 8, $avail, $type);
                return \array_values(\unpack('P*', \substr($raw, $p, $n * 8)));
            case PhysicalType::Float:
                self::need($n * 4, $avail, $type);
                return \array_values(\unpack('g*', \substr($raw, $p, $n * 4)));
            case PhysicalType::Double:
                self::need($n * 8, $avail, $type);
                return \array_values(\unpack('e*', \substr($raw, $p, $n * 8)));
            case PhysicalType::Int96:
                self::need($n * 12, $avail, $type);
                return \str_split(\substr($raw, $p, $n * 12), 12);
            case PhysicalType::FixedLenByteArray:
                $l = $typeLength ?? throw new ParquetException('FIXED_LEN_BYTE_ARRAY without a length');
                self::need($n * $l, $avail, $type);
                return \str_split(\substr($raw, $p, $n * $l), $l);
            case PhysicalType::Boolean:
                self::need(($n + 7) >> 3, $avail, $type);
                $v = [];
                for ($i = 0; $i < $n; $i++) {
                    $v[] = ((\ord($raw[$p + ($i >> 3)]) >> ($i & 7)) & 1) === 1;
                }
                return $v;
            case PhysicalType::ByteArray:
                $v = [];
                for ($i = 0; $i < $n; $i++) {
                    if ($p + 4 > $rawLen) {
                        throw new ParquetException('A BYTE_ARRAY length is cut off');
                    }
                    $l = \unpack('V', $raw, $p)[1];
                    $p += 4;
                    if ($l > $rawLen - $p) {
                        throw new ParquetException('A BYTE_ARRAY value runs past the end of its page');
                    }
                    $v[] = \substr($raw, $p, $l);
                    $p += $l;
                }
                return $v;
        }
    }

    private static ?bool $le = null;

    /** unpack()'s 'l' is in machine byte order; on a big-endian host INT32 is read as unsigned little-endian and then sign-corrected. */
    private static function littleEndian(): bool
    {
        return self::$le ??= \pack('S', 1) === "\x01\x00";
    }

    private static function need(int $bytes, int $avail, PhysicalType $type): void
    {
        if ($bytes > $avail) {
            throw new ParquetException("PLAIN {$type->name} values need $bytes bytes, the page has $avail");
        }
    }
}
