<?php

declare(strict_types=1);

namespace Charm\Parquet\Internal;

use Charm\Parquet\{FormatException, PhysicalType};

/** BYTE_STREAM_SPLIT: byte k of every value is stored together, so $n values of width W are W streams of $n bytes. */
final class ByteStreamSplit
{
    /** @return list<int|float|string> */
    public static function decode(string $raw, int $p, PhysicalType $type, int $n, ?int $typeLength): array
    {
        if ($n <= 0) {
            return [];
        }
        $w = match ($type) {
            PhysicalType::Int32, PhysicalType::Float => 4,
            PhysicalType::Int64, PhysicalType::Double => 8,
            PhysicalType::FixedLenByteArray => $typeLength ?? throw new FormatException('FIXED_LEN_BYTE_ARRAY without a length'),
            default => throw new FormatException("BYTE_STREAM_SPLIT cannot encode {$type->name}"),
        };
        $avail = \strlen($raw) - $p;
        if ($n > \intdiv($avail, $w)) {
            throw new FormatException("BYTE_STREAM_SPLIT values need " . ($n * $w) . " bytes, the page has $avail");
        }
        $s = [];
        for ($k = 0; $k < $w; $k++) {
            $s[$k] = \substr($raw, $p + $k * $n, $n);
        }
        if ($type === PhysicalType::FixedLenByteArray) {
            $out = [];
            for ($i = 0; $i < $n; $i++) {
                $v = '';
                for ($k = 0; $k < $w; $k++) {
                    $v .= $s[$k][$i];
                }
                $out[] = $v;
            }
            return $out;
        }
        // interleave once, then decode the whole page with a single unpack()
        $inter = '';
        if ($w === 4) {
            [$a, $b, $c, $d] = $s;
            for ($i = 0; $i < $n; $i++) {
                $inter .= $a[$i] . $b[$i] . $c[$i] . $d[$i];
            }
        } else {
            [$a, $b, $c, $d, $e, $f, $g, $h] = $s;
            for ($i = 0; $i < $n; $i++) {
                $inter .= $a[$i] . $b[$i] . $c[$i] . $d[$i] . $e[$i] . $f[$i] . $g[$i] . $h[$i];
            }
        }
        $fmt = match ($type) {
            PhysicalType::Float => 'g*',
            PhysicalType::Double => 'e*',
            PhysicalType::Int64 => 'P*',
            default => 'V*',
        };
        $v = \array_values(\unpack($fmt, $inter));
        if ($type === PhysicalType::Int32) {
            foreach ($v as $i => $x) {
                if ($x >= 0x80000000) {
                    $v[$i] = $x - 0x100000000;
                }
            }
        }
        return $v;
    }
}
