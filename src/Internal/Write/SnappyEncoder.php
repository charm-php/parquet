<?php

declare(strict_types=1);

namespace Charm\Parquet\Internal\Write;

/**
 * A Snappy compressor (the raw, unframed format): a greedy matcher over a hash table of 4-byte sequences. Its output is valid for any
 * Snappy decoder; the ratio is below the reference implementation's (no second chance for a match, no skip heuristics beyond the first).
 */
final class SnappyEncoder
{
    /** @var array<int, int> the hash table, kept between calls (an entry is only a candidate: it is verified before it is used) */
    private static array $table = [];

    public static function compress(string $in): string
    {
        $n = \strlen($in);
        $out = self::varint($n);
        if ($n === 0) {
            return $out;
        }
        if (self::$table === []) {
            self::$table = \array_fill(0, 16384, -1);
        }
        $table = &self::$table;
        $anchor = 0;
        $p = 0;
        $limit = $n - 4;
        while ($p <= $limit) {
            $v = \unpack('V', $in, $p)[1];
            $h = (($v * 0x1E35A7BD) & 0xFFFFFFFF) >> 18;
            $cand = $table[$h];
            $table[$h] = $p;
            if ($cand >= 0 && $cand < $p && \unpack('V', $in, $cand)[1] === $v) {
                $len = 4;
                $max = $n - $p;
                while ($len < $max && $in[$cand + $len] === $in[$p + $len]) {
                    ++$len;
                }
                if ($p > $anchor) {
                    $out .= self::literal($in, $anchor, $p - $anchor);
                }
                $offset = $p - $cand;
                $out .= self::copy($offset, $len);
                $p += $len;
                $anchor = $p;
                continue;
            }
            $p += 1 + (($p - $anchor) >> 5); // (skip faster through data that does not repeat)
        }
        if ($anchor < $n) {
            $out .= self::literal($in, $anchor, $n - $anchor);
        }
        return $out;
    }

    private static function literal(string $in, int $from, int $len): string
    {
        $l = $len - 1;
        if ($l < 60) {
            return \chr($l << 2) . \substr($in, $from, $len);
        }
        $head = '';
        $bytes = 0;
        for ($x = $l; $x > 0; $x >>= 8) {
            $head .= \chr($x & 0xFF);
            ++$bytes;
        }
        return \chr((59 + $bytes) << 2) . $head . \substr($in, $from, $len);
    }

    /** A copy of $len bytes from $offset back, as copies of at most 64 bytes. */
    private static function copy(int $offset, int $len): string
    {
        $out = '';
        while ($len > 0) {
            $c = \min($len, 64);
            $out .= $offset < 65536
                ? \chr((($c - 1) << 2) | 2) . \pack('v', $offset)
                : \chr((($c - 1) << 2) | 3) . \pack('V', $offset);
            $len -= $c;
        }
        return $out;
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
