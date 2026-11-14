<?php

declare(strict_types=1);

namespace Charm\Parquet\Internal;

use Charm\Parquet\ParquetException;

/** The RLE / bit-packed hybrid encoding of levels and dictionary indices. */
final class RleHybrid
{
    /**
     * Exactly $count values of $bitWidth bits from $raw[$p .. $end). $max receives the largest value read.
     *
     * @return list<int>
     */
    public static function decode(string $raw, int $p, int $end, int $bitWidth, int $count, int &$max): array
    {
        $max = 0;
        if ($bitWidth < 0 || $bitWidth > 32) {
            throw new ParquetException("RLE bit width $bitWidth is not valid");
        }
        if ($end > \strlen($raw) || $p > $end) {
            throw new ParquetException('RLE data lies outside the page');
        }
        if ($count <= 0) {
            return [];
        }
        $bytes = ($bitWidth + 7) >> 3;
        $mask = (1 << $bitWidth) - 1;
        $segs = [];
        $n = 0;
        while ($n < $count) {
            if ($p >= $end) {
                throw new ParquetException('RLE/bit-packed data ends before all values are read');
            }
            $h = 0;
            for ($shift = 0; ; $shift += 7) {
                if ($p >= $end || $shift > 28) {
                    throw new ParquetException('RLE run header is cut off or too long');
                }
                $b = \ord($raw[$p++]);
                $h |= ($b & 0x7F) << $shift;
                if (($b & 0x80) === 0) {
                    break;
                }
            }
            if (($h & 1) !== 0) {
                // bit-packed: ($h >> 1) groups of 8 values, least significant bits first
                $groups = $h >> 1;
                $take = \min($groups * 8, $count - $n);
                $need = ($take * $bitWidth + 7) >> 3;
                if ($need > $end - $p) {
                    throw new ParquetException('Bit-packed run reaches past the end of its data');
                }
                if ($bitWidth === 0) {
                    $segs[] = \array_fill(0, $take, 0);
                    $n += $take;
                    continue;
                }
                $vals = [];
                $acc = 0;
                $bits = 0;
                $q = $p;
                for ($i = 0; $i < $take; $i++) {
                    while ($bits < $bitWidth) {
                        $acc |= \ord($raw[$q++]) << $bits;
                        $bits += 8;
                    }
                    $v = $acc & $mask;
                    $acc >>= $bitWidth;
                    $bits -= $bitWidth;
                    $vals[] = $v;
                    if ($v > $max) {
                        $max = $v;
                    }
                }
                $segs[] = $vals;
                $p += $need;
                $n += $take;
            } else {
                // a run: ($h >> 1) copies of one value, stored in the fewest whole bytes
                if ($bytes > $end - $p) {
                    throw new ParquetException('RLE run value is cut off');
                }
                $v = 0;
                for ($i = 0; $i < $bytes; $i++) {
                    $v |= \ord($raw[$p + $i]) << (8 * $i);
                }
                $p += $bytes;
                $run = \min($h >> 1, $count - $n);
                if ($run > 0) {
                    $segs[] = \array_fill(0, $run, $v);
                    if ($v > $max) {
                        $max = $v;
                    }
                    $n += $run;
                }
            }
        }
        return \count($segs) === 1 ? $segs[0] : \array_merge(...$segs);
    }

    /**
     * Dictionary indices looked up as they are decoded: $count values of $bitWidth bits from $raw[$p .. $end), each replaced by
     * $dict[index]. The same stream as decode(); the second pass over an array of indices is saved. An index outside the dictionary
     * is a ParquetException.
     *
     * @param list<mixed> $dict
     * @return list<mixed>
     */
    public static function decodeDictionary(string $raw, int $p, int $end, int $bitWidth, int $count, array $dict): array
    {
        if ($bitWidth < 0 || $bitWidth > 32) {
            throw new ParquetException("RLE bit width $bitWidth is not valid");
        }
        if ($end > \strlen($raw) || $p > $end) {
            throw new ParquetException('RLE data lies outside the page');
        }
        if ($count <= 0) {
            return [];
        }
        $size = \count($dict);
        $bytes = ($bitWidth + 7) >> 3;
        $mask = (1 << $bitWidth) - 1;
        $segs = [];
        $n = 0;
        while ($n < $count) {
            if ($p >= $end) {
                throw new ParquetException('RLE/bit-packed data ends before all values are read');
            }
            $h = 0;
            for ($shift = 0; ; $shift += 7) {
                if ($p >= $end || $shift > 28) {
                    throw new ParquetException('RLE run header is cut off or too long');
                }
                $b = \ord($raw[$p++]);
                $h |= ($b & 0x7F) << $shift;
                if (($b & 0x80) === 0) {
                    break;
                }
            }
            if (($h & 1) !== 0) {
                $groups = $h >> 1;
                $take = \min($groups * 8, $count - $n);
                $need = ($take * $bitWidth + 7) >> 3;
                if ($need > $end - $p) {
                    throw new ParquetException('Bit-packed run reaches past the end of its data');
                }
                if ($bitWidth === 0) {
                    if ($take > 0 && $size === 0) {
                        throw new ParquetException('A dictionary index points into an empty dictionary');
                    }
                    $segs[] = \array_fill(0, $take, $dict[0] ?? null);
                    $n += $take;
                    continue;
                }
                $vals = [];
                $acc = 0;
                $bits = 0;
                $q = $p;
                for ($i = 0; $i < $take; $i++) {
                    while ($bits < $bitWidth) {
                        $acc |= \ord($raw[$q++]) << $bits;
                        $bits += 8;
                    }
                    $v = $acc & $mask;
                    $acc >>= $bitWidth;
                    $bits -= $bitWidth;
                    if ($v >= $size) {
                        throw new ParquetException("A dictionary index $v is outside the dictionary of $size values");
                    }
                    $vals[] = $dict[$v];
                }
                $segs[] = $vals;
                $p += $need;
                $n += $take;
            } else {
                if ($bytes > $end - $p) {
                    throw new ParquetException('RLE run value is cut off');
                }
                $v = 0;
                for ($i = 0; $i < $bytes; $i++) {
                    $v |= \ord($raw[$p + $i]) << (8 * $i);
                }
                $p += $bytes;
                $run = \min($h >> 1, $count - $n);
                if ($run > 0) {
                    if ($v >= $size) {
                        throw new ParquetException("A dictionary index $v is outside the dictionary of $size values");
                    }
                    $segs[] = \array_fill(0, $run, $dict[$v]);
                    $n += $run;
                }
            }
        }
        return \count($segs) === 1 ? $segs[0] : \array_merge(...$segs);
    }
}
