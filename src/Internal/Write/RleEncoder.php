<?php

declare(strict_types=1);

namespace Charm\Parquet\Internal\Write;

/** The RLE / bit-packed hybrid encoding of levels and dictionary indices. */
final class RleEncoder
{
    /**
     * @param list<int> $vals values of at most $bitWidth bits
     * @return string the runs, without any length prefix
     */
    public static function encode(array $vals, int $bitWidth): string
    {
        $n = \count($vals);
        if ($n === 0) {
            return '';
        }
        $bytes = \intdiv($bitWidth + 7, 8);
        $out = '';
        $pending = [];
        $i = 0;
        while ($i < $n) {
            $v = $vals[$i];
            $j = $i + 1;
            while ($j < $n && $vals[$j] === $v) {
                ++$j;
            }
            $run = $j - $i;
            $i = $j;
            if ($run >= 8) {
                // a bit-packed run holds whole groups of 8: top up the pending values from this run, then the rest is an RLE run
                $take = (8 - \count($pending) % 8) % 8;
                if ($run - $take >= 8) {
                    for ($k = 0; $k < $take; $k++) {
                        $pending[] = $v;
                    }
                    if ($pending !== []) {
                        $out .= self::packed($pending, $bitWidth);
                        $pending = [];
                    }
                    $out .= self::varint(($run - $take) << 1) . \substr(\pack('V', $v), 0, $bytes);
                    continue;
                }
            }
            for ($k = 0; $k < $run; $k++) {
                $pending[] = $v;
            }
        }
        if ($pending !== []) {
            while (\count($pending) % 8 !== 0) {
                $pending[] = 0; // (padding of the last group: the reader stops at the value count)
            }
            $out .= self::packed($pending, $bitWidth);
        }
        return $out;
    }

    /** One bit-packed run of $vals (a multiple of 8 values). @param list<int> $vals */
    private static function packed(array $vals, int $bitWidth): string
    {
        $groups = \intdiv(\count($vals), 8);
        $head = self::varint(($groups << 1) | 1);
        if ($bitWidth === 1) {
            $s = '';
            for ($g = 0, $i = 0; $g < $groups; $g++, $i += 8) {
                $s .= \chr($vals[$i] | ($vals[$i + 1] << 1) | ($vals[$i + 2] << 2) | ($vals[$i + 3] << 3) | ($vals[$i + 4] << 4) | ($vals[$i + 5] << 5) | ($vals[$i + 6] << 6) | ($vals[$i + 7] << 7));
            }
            return $head . $s;
        }
        if ($bitWidth === 8) {
            return $head . \pack('C*', ...$vals);
        }
        $s = '';
        $acc = 0;
        $bits = 0;
        foreach ($vals as $v) {
            $acc |= $v << $bits;
            $bits += $bitWidth;
            while ($bits >= 8) {
                $s .= \chr($acc & 0xFF);
                $acc >>= 8;
                $bits -= 8;
            }
        }
        return $head . $s; // (8 values of $bitWidth bits are a whole number of bytes: nothing is left in $acc)
    }

    private static function varint(int $v): string
    {
        $out = '';
        do {
            $b = $v & 0x7F;
            $v >>= 7;
            $out .= \chr($v !== 0 ? $b | 0x80 : $b);
        } while ($v !== 0);
        return $out;
    }
}
